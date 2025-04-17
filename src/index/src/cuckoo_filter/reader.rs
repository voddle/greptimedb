// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Range;
use std::str;

use async_trait::async_trait;
use bytes::Bytes;
use common_base::range_read::RangeReader;
use cuckoofilter::CuckooFilter;
use greptime_proto::v1::index::{CuckooFilterLoc, CuckooFilterMeta};
use prost::Message;
use snafu::{ensure, ResultExt};
use std::collections::hash_map::DefaultHasher;

use crate::cuckoo_filter::error::{
    DecodeProtoSnafu, FileSizeTooSmallSnafu, IoSnafu, Result, UnexpectedMetaSizeSnafu,
};
use crate::cuckoo_filter::SEED;

/// Minimum size of the cuckoo filter, which is the size of the length of the cuckoo filter.
const BLOOM_META_LEN_SIZE: u64 = 4;

/// Default prefetch size of cuckoo filter meta.
pub const DEFAULT_PREFETCH_SIZE: u64 = 8192; // 8KiB

/// `CuckooFilterReader` reads the cuckoo filter from the file.
#[async_trait]
pub trait CuckooFilterReader: Sync {
    /// Reads range of bytes from the file.
    async fn range_read(&self, offset: u64, size: u32) -> Result<Bytes>;

    /// Reads bunch of ranges from the file.
    async fn read_vec(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            let size = (range.end - range.start) as u32;
            let data = self.range_read(range.start, size).await?;
            results.push(data);
        }
        Ok(results)
    }

    /// Reads the meta information of the cuckoo filter.
    async fn metadata(&self) -> Result<CuckooFilterMeta>;

    /// Reads a cuckoo filter with the given location.
    async fn cuckoo_filter(&self, loc: &CuckooFilterLoc) -> Result<CuckooFilter<DefaultHasher>> {
        let bytes = self.range_read(loc.offset, loc.size as _).await?;
        // let vec = bytes
        //     .chunks_exact(std::mem::size_of::<u8>())
        //     .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        //     .collect();
        let vec: Vec<u8> = bytes.to_vec();
        // let s = str::from_utf8(&bytes).unwrap();
        // let json: cuckoofilter::ExportedCuckooFilter = serde_json::from_str(&s).unwrap();
        let export_cf = cuckoofilter::ExportedCuckooFilter{
            values: vec,
            length: loc.element_count as _,
        };
        let bm = CuckooFilter::from(export_cf);
        Ok(bm)
    }

    async fn cuckoo_filter_vec(&self, locs: &[CuckooFilterLoc]) -> Result<Vec<CuckooFilter<DefaultHasher>>> {
        let ranges = locs
            .iter()
            .map(|l| l.offset..l.offset + l.size)
            .collect::<Vec<_>>();
        let bss = self.read_vec(&ranges).await?;

        let mut result = Vec::with_capacity(bss.len());
        for (bs, loc) in bss.into_iter().zip(locs.iter()) {
            // let vec = bs
            //     .chunks_exact(std::mem::size_of::<u64>())
            //     .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            //     .collect();
            let bytes = self.range_read(loc.offset, loc.size as _).await?;
            let export_cf = cuckoofilter::ExportedCuckooFilter{
                values: bytes.to_vec(),
                length: loc.element_count as _,
            };
            let bm = CuckooFilter::from(export_cf);
            result.push(bm);
        }

        Ok(result)
    }
}

/// `CuckooFilterReaderImpl` reads the cuckoo filter from the file.
pub struct CuckooFilterReaderImpl<R: RangeReader> {
    /// The underlying reader.
    reader: R,
}

impl<R: RangeReader> CuckooFilterReaderImpl<R> {
    /// Creates a new `CuckooFilterReaderImpl` with the given reader.
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl<R: RangeReader> CuckooFilterReader for CuckooFilterReaderImpl<R> {
    async fn range_read(&self, offset: u64, size: u32) -> Result<Bytes> {
        self.reader
            .read(offset..offset + size as u64)
            .await
            .context(IoSnafu)
    }

    async fn read_vec(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.reader.read_vec(ranges).await.context(IoSnafu)
    }

    async fn metadata(&self) -> Result<CuckooFilterMeta> {
        let metadata = self.reader.metadata().await.context(IoSnafu)?;
        let file_size = metadata.content_length;

        let mut meta_reader =
            CuckooFilterMetaReader::new(&self.reader, file_size, Some(DEFAULT_PREFETCH_SIZE));
        meta_reader.metadata().await
    }
}

/// `CuckooFilterMetaReader` reads the metadata of the cuckoo filter.
struct CuckooFilterMetaReader<R: RangeReader> {
    reader: R,
    file_size: u64,
    prefetch_size: u64,
}

impl<R: RangeReader> CuckooFilterMetaReader<R> {
    pub fn new(reader: R, file_size: u64, prefetch_size: Option<u64>) -> Self {
        Self {
            reader,
            file_size,
            prefetch_size: prefetch_size
                .unwrap_or(BLOOM_META_LEN_SIZE)
                .max(BLOOM_META_LEN_SIZE),
        }
    }

    /// Reads the metadata of the cuckoo filter.
    ///
    /// It will first prefetch some bytes from the end of the file,
    /// then parse the metadata from the prefetch bytes.
    pub async fn metadata(&mut self) -> Result<CuckooFilterMeta> {
        ensure!(
            self.file_size >= BLOOM_META_LEN_SIZE,
            FileSizeTooSmallSnafu {
                size: self.file_size,
            }
        );

        let meta_start = self.file_size.saturating_sub(self.prefetch_size);
        let suffix = self
            .reader
            .read(meta_start..self.file_size)
            .await
            .context(IoSnafu)?;
        let suffix_len = suffix.len();
        let length = u32::from_le_bytes(Self::read_tailing_four_bytes(&suffix)?) as u64;
        self.validate_meta_size(length)?;

        if length > suffix_len as u64 - BLOOM_META_LEN_SIZE {
            let metadata_start = self.file_size - length - BLOOM_META_LEN_SIZE;
            let meta = self
                .reader
                .read(metadata_start..self.file_size - BLOOM_META_LEN_SIZE)
                .await
                .context(IoSnafu)?;
            CuckooFilterMeta::decode(meta).context(DecodeProtoSnafu)
        } else {
            let metadata_start = self.file_size - length - BLOOM_META_LEN_SIZE - meta_start;
            let meta = &suffix[metadata_start as usize..suffix_len - BLOOM_META_LEN_SIZE as usize];
            CuckooFilterMeta::decode(meta).context(DecodeProtoSnafu)
        }
    }

    fn read_tailing_four_bytes(suffix: &[u8]) -> Result<[u8; 4]> {
        let suffix_len = suffix.len();
        ensure!(
            suffix_len >= 4,
            FileSizeTooSmallSnafu {
                size: suffix_len as u64
            }
        );
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&suffix[suffix_len - 4..suffix_len]);

        Ok(bytes)
    }

    fn validate_meta_size(&self, length: u64) -> Result<()> {
        let max_meta_size = self.file_size - BLOOM_META_LEN_SIZE;
        ensure!(
            length <= max_meta_size,
            UnexpectedMetaSizeSnafu {
                max_meta_size,
                actual_meta_size: length,
            }
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::io::Cursor;

    use super::*;
    use crate::cuckoo_filter::creator::CuckooFilterCreator;
    use crate::external_provider::MockExternalTempFileProvider;

    async fn mock_cuckoo_filter_bytes() -> Vec<u8> {
        let mut writer = Cursor::new(vec![]);
        let mut creator = CuckooFilterCreator::new(
            2,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        creator
            .push_row_elems(vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        creator
            .push_row_elems(vec![b"c".to_vec(), b"d".to_vec()])
            .await
            .unwrap();
        creator
            .push_row_elems(vec![b"e".to_vec(), b"f".to_vec()])
            .await
            .unwrap();

        creator.finish(&mut writer).await.unwrap();

        writer.into_inner()
    }

    #[tokio::test]
    async fn test_cuckoo_filter_meta_reader() {
        let bytes = mock_cuckoo_filter_bytes().await;
        let file_size = bytes.len() as u64;

        for prefetch in [0u64, file_size / 2, file_size, file_size + 10] {
            let mut reader =
                CuckooFilterMetaReader::new(bytes.clone(), file_size as _, Some(prefetch));
            let meta = reader.metadata().await.unwrap();

            assert_eq!(meta.rows_per_segment, 2);
            assert_eq!(meta.segment_count, 2);
            assert_eq!(meta.row_count, 3);
            assert_eq!(meta.cuckoo_filter_locs.len(), 2);

            assert_eq!(meta.cuckoo_filter_locs[0].offset, 0);
            assert_eq!(meta.cuckoo_filter_locs[0].element_count, 4);
            assert_eq!(
                meta.cuckoo_filter_locs[1].offset,
                meta.cuckoo_filter_locs[0].size
            );
            assert_eq!(meta.cuckoo_filter_locs[1].element_count, 2);
        }
    }

    #[tokio::test]
    async fn test_cuckoo_filter_reader() {
        let bytes = mock_cuckoo_filter_bytes().await;

        let reader = CuckooFilterReaderImpl::new(bytes);
        let meta = reader.metadata().await.unwrap();

        assert_eq!(meta.cuckoo_filter_locs.len(), 2);
        let bf = reader
            .cuckoo_filter(&meta.cuckoo_filter_locs[0])
            .await
            .unwrap();
        assert!(bf.contains(&b"a"));
        assert!(bf.contains(&b"b"));
        assert!(bf.contains(&b"c"));
        assert!(bf.contains(&b"d"));

        let bf = reader
            .cuckoo_filter(&meta.cuckoo_filter_locs[1])
            .await
            .unwrap();
        assert!(bf.contains(&b"e"));
        assert!(bf.contains(&b"f"));
    }
}

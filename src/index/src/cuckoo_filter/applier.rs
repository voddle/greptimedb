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

use std::collections::HashSet;
use std::ops::Range;

use greptime_proto::v1::index::CuckooFilterMeta;
use itertools::Itertools;

use crate::cuckoo_filter::error::Result;
use crate::cuckoo_filter::reader::CuckooFilterReader;
use crate::Bytes;

pub struct CuckooFilterApplier {
    reader: Box<dyn CuckooFilterReader + Send>,
    meta: CuckooFilterMeta,
}

impl CuckooFilterApplier {
    pub async fn new(reader: Box<dyn CuckooFilterReader + Send>) -> Result<Self> {
        let meta = reader.metadata().await?;

        Ok(Self { reader, meta })
    }

    /// Searches ranges of rows that match the given probes in the given search range.
    pub async fn search(
        &mut self,
        probes: &HashSet<Bytes>,
        search_range: Range<usize>,
    ) -> Result<Vec<Range<usize>>> {
        let rows_per_segment = self.meta.rows_per_segment as usize;
        let start_seg = search_range.start / rows_per_segment;
        let mut end_seg = search_range.end.div_ceil(rows_per_segment);

        if end_seg == self.meta.segment_loc_indices.len() + 1 {
            // In a previous version, there was a bug where if the last segment was all null,
            // this segment would not be written into the index. This caused the slice
            // `self.meta.segment_loc_indices[start_seg..end_seg]` to go out of bounds due to
            // the missing segment. Since the `search` function does not search for nulls,
            // we can simply ignore the last segment in this buggy scenario.
            end_seg -= 1;
        }

        let locs = &self.meta.segment_loc_indices[start_seg..end_seg];

        // dedup locs
        let deduped_locs = locs
            .iter()
            .dedup()
            .map(|i| self.meta.cuckoo_filter_locs[*i as usize])
            .collect::<Vec<_>>();
        let cfs = self.reader.cuckoo_filter_vec(&deduped_locs).await?;

        let mut ranges: Vec<Range<usize>> = Vec::with_capacity(cfs.len());
        for ((_, mut group), cuckoo) in locs
            .iter()
            .zip(start_seg..end_seg)
            .chunk_by(|(x, _)| **x)
            .into_iter()
            .zip(cfs.iter())
        {
            let start = group.next().unwrap().1 * rows_per_segment; // SAFETY: group is not empty
            let end = group.last().map_or(start + rows_per_segment, |(_, end)| {
                (end + 1) * rows_per_segment
            });
            let actual_start = start.max(search_range.start);
            let actual_end = end.min(search_range.end);
            for probe in probes {
                if cuckoo.contains(probe) {
                    match ranges.last_mut() {
                        Some(last) if last.end == actual_start => {
                            last.end = actual_end;
                        }
                        _ => {
                            ranges.push(actual_start..actual_end);
                        }
                    }
                    break;
                }
            }
        }

        Ok(ranges)
    }

    /// Deletes the specified probes from the Cuckoo filter in the given search range.
    /// Returns the ranges where the deletion was successful.
    pub async fn delete(
        &mut self,
        probes: &HashSet<Bytes>,
        search_range: Range<usize>,
    ) -> Result<Vec<Range<usize>>> {
        let rows_per_segment = self.meta.rows_per_segment as usize;
        let start_seg = search_range.start / rows_per_segment;
        let mut end_seg = search_range.end.div_ceil(rows_per_segment);

        if end_seg == self.meta.segment_loc_indices.len() + 1 {
            end_seg -= 1;
        }

        let locs = &self.meta.segment_loc_indices[start_seg..end_seg];

        // dedup locs
        let deduped_locs = locs
            .iter()
            .dedup()
            .map(|i| self.meta.cuckoo_filter_locs[*i as usize])
            .collect::<Vec<_>>();
        let cfs = self.reader.cuckoo_filter_vec(&deduped_locs).await?;

        let mut ranges: Vec<Range<usize>> = Vec::with_capacity(cfs.len());
        for ((_, mut group), cuckoo) in locs
            .iter()
            .zip(start_seg..end_seg)
            .chunk_by(|(x, _)| **x)
            .into_iter()
            .zip(cfs.iter())
        {
            let start = group.next().unwrap().1 * rows_per_segment;
            let end = group.last().map_or(start + rows_per_segment, |(_, end)| {
                (end + 1) * rows_per_segment
            });
            let actual_start = start.max(search_range.start);
            let actual_end = end.min(search_range.end);

            let mut deleted = false;
            for probe in probes {
                if cuckoo.contains(probe) {
                    // Attempt to delete the probe
                    if cuckoo.delete(probe) {
                        deleted = true;
                        break;
                    }
                }
            }

            if deleted {
                match ranges.last_mut() {
                    Some(last) if last.end == actual_start => {
                        last.end = actual_end;
                    }
                    _ => {
                        ranges.push(actual_start..actual_end);
                    }
                }
            }
        }

        Ok(ranges)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::io::Cursor;

    use super::*;
    use crate::cuckoo_filter::creator::CuckooFilterCreator;
    use crate::cuckoo_filter::reader::CuckooFilterReaderImpl;
    use crate::external_provider::MockExternalTempFileProvider;

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_appliter() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = CuckooFilterCreator::new(
            4,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        let rows = vec![
            // seg 0
            vec![b"row00".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row01".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row02".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row03".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            // seg 1
            vec![b"row04".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row05".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row06".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            vec![b"row07".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            // seg 2
            vec![b"row08".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row09".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row10".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row11".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            // duplicate rows
            // seg 3
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 4
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 5
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 6
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
        ];

        let cases = vec![
            (vec![b"row00".to_vec()], 0..28, vec![0..4]), // search one row in full range
            (vec![b"row05".to_vec()], 4..8, vec![4..8]),  // search one row in partial range
            (vec![b"row03".to_vec()], 4..8, vec![]), // search for a row that doesn't exist in the partial range
            (
                vec![b"row01".to_vec(), b"row06".to_vec()],
                0..28,
                vec![0..8],
            ), // search multiple rows in multiple ranges
            (
                vec![b"row01".to_vec(), b"row11".to_vec()],
                0..28,
                vec![0..4, 8..12],
            ), // search multiple rows in multiple ranges
            (vec![b"row99".to_vec()], 0..28, vec![]), // search for a row that doesn't exist in the full range
            (vec![b"row00".to_vec()], 12..12, vec![]), // search in an empty range
            (
                vec![b"row04".to_vec(), b"row05".to_vec()],
                0..12,
                vec![4..8],
            ), // search multiple rows in same segment
            (vec![b"seg01".to_vec()], 0..28, vec![4..8]), // search rows in a segment
            (vec![b"seg01".to_vec()], 6..28, vec![6..8]), // search rows in a segment in partial range
            (vec![b"overl".to_vec()], 0..28, vec![0..8]), // search rows in multiple segments
            (vec![b"overl".to_vec()], 2..28, vec![2..8]), // search range starts from the middle of a segment
            (vec![b"overp".to_vec()], 0..10, vec![4..10]), // search range ends at the middle of a segment
            (vec![b"dup".to_vec()], 0..12, vec![]), // search for a duplicate row not in the range
            (vec![b"dup".to_vec()], 0..16, vec![12..16]), // search for a duplicate row in the range
            (vec![b"dup".to_vec()], 0..28, vec![12..28]), // search for a duplicate row in the full range
        ];

        for row in rows {
            creator.push_row_elems(row).await.unwrap();
        }

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();

        let reader = CuckooFilterReaderImpl::new(bytes);

        let mut applier = CuckooFilterApplier::new(Box::new(reader)).await.unwrap();

        for (probes, search_range, expected) in cases {
            let probes: HashSet<Bytes> = probes.into_iter().collect();
            let ranges = applier.search(&probes, search_range).await.unwrap();
            assert_eq!(ranges, expected);
        }
    }
}
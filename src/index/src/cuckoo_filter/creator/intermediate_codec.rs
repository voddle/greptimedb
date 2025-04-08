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

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use asynchronous_codec::{Decoder, Encoder, FramedRead, FramedWrite};
use bytes::{Buf, BytesMut};
use futures::{AsyncRead, AsyncWrite};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::Decoder as _;

use crate::cuckoo_filter::creator::finalize_segment::FinalizedCuckooFilterSegment;

/// The version of the intermediate Cuckoo filter codec.
const INTERMEDIATE_CODEC_VERSION: u8 = 1;

/// Codec for encoding and decoding intermediate Cuckoo filters.
#[derive(Debug, Clone, Default)]
pub struct IntermediateCuckooFilterCodecV1 {
    /// The underlying length-delimited codec.
    inner: LengthDelimitedCodec,
}

impl Encoder<FinalizedCuckooFilterSegment> for IntermediateCuckooFilterCodecV1 {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: FinalizedCuckooFilterSegment,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        // Write version
        dst.put_u8(INTERMEDIATE_CODEC_VERSION);

        // Write element count
        dst.put_u64_le(item.element_count as u64);

        // Write Cuckoo filter bytes
        dst.extend_from_slice(&item.cuckoo_filter_bytes);

        Ok(())
    }
}

impl Decoder for IntermediateCuckooFilterCodecV1 {
    type Item = FinalizedCuckooFilterSegment;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Read version
        if src.len() < 1 {
            return Ok(None);
        }
        let version = src.get_u8();
        if version != INTERMEDIATE_CODEC_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported codec version: {}", version),
            ));
        }

        // Read element count
        if src.len() < 8 {
            return Ok(None);
        }
        let element_count = src.get_u64_le() as usize;

        // Read Cuckoo filter bytes
        if src.is_empty() {
            return Ok(None);
        }
        let cuckoo_filter_bytes = src.to_vec();
        src.clear();

        Ok(Some(FinalizedCuckooFilterSegment {
            cuckoo_filter_bytes,
            element_count,
        }))
    }
}

/// A framed reader for intermediate Cuckoo filters.
pub type IntermediateCuckooFilterReader<R> = FramedRead<R, IntermediateCuckooFilterCodecV1>;

/// A framed writer for intermediate Cuckoo filters.
pub type IntermediateCuckooFilterWriter<W> = FramedWrite<W, IntermediateCuckooFilterCodecV1>;

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::Poll;

    use futures::{AsyncRead, AsyncWrite, StreamExt};
    use tokio::io::duplex;
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    use super::*;
    use crate::cuckoo_filter::creator::finalize_segment::FinalizedCuckooFilterSegment;

    #[tokio::test]
    async fn test_intermediate_cuckoo_filter_codec() {
        let (writer, reader) = duplex(2 * 1024 * 1024);
        let writer = writer.compat_write();
        let reader = reader.compat();

        let mut framed_writer = IntermediateCuckooFilterWriter::new(writer, IntermediateCuckooFilterCodecV1::default());
        let mut framed_reader = IntermediateCuckooFilterReader::new(reader, IntermediateCuckooFilterCodecV1::default());

        let segment = FinalizedCuckooFilterSegment {
            cuckoo_filter_bytes: vec![1, 2, 3, 4],
            element_count: 100,
        };

        framed_writer.send(segment.clone()).await.unwrap();
        framed_writer.flush().await.unwrap();

        let decoded = framed_reader.next().await.unwrap().unwrap();
        assert_eq!(decoded.cuckoo_filter_bytes, segment.cuckoo_filter_bytes);
        assert_eq!(decoded.element_count, segment.element_count);
    }

    #[tokio::test]
    async fn test_intermediate_cuckoo_filter_codec_multiple() {
        let (writer, reader) = duplex(2 * 1024 * 1024);
        let writer = writer.compat_write();
        let reader = reader.compat();

        let mut framed_writer = IntermediateCuckooFilterWriter::new(writer, IntermediateCuckooFilterCodecV1::default());
        let mut framed_reader = IntermediateCuckooFilterReader::new(reader, IntermediateCuckooFilterCodecV1::default());

        let segments = vec![
            FinalizedCuckooFilterSegment {
                cuckoo_filter_bytes: vec![1, 2, 3, 4],
                element_count: 100,
            },
            FinalizedCuckooFilterSegment {
                cuckoo_filter_bytes: vec![5, 6, 7, 8],
                element_count: 200,
            },
        ];

        for segment in &segments {
            framed_writer.send(segment.clone()).await.unwrap();
        }
        framed_writer.flush().await.unwrap();

        for expected in segments {
            let decoded = framed_reader.next().await.unwrap().unwrap();
            assert_eq!(decoded.cuckoo_filter_bytes, expected.cuckoo_filter_bytes);
            assert_eq!(decoded.element_count, expected.element_count);
        }
    }

    #[tokio::test]
    async fn test_intermediate_cuckoo_filter_codec_invalid_version() {
        let (writer, reader) = duplex(2 * 1024 * 1024);
        let writer = writer.compat_write();
        let reader = reader.compat();

        let mut framed_writer = IntermediateCuckooFilterWriter::new(writer, IntermediateCuckooFilterCodecV1::default());
        let mut framed_reader = IntermediateCuckooFilterReader::new(reader, IntermediateCuckooFilterCodecV1::default());

        // Write invalid version
        let mut bytes = BytesMut::new();
        bytes.put_u8(2); // Invalid version
        bytes.put_u64_le(100); // Element count
        bytes.extend_from_slice(&[1, 2, 3, 4]); // Cuckoo filter bytes

        framed_writer.get_mut().write_all(&bytes).await.unwrap();
        framed_writer.flush().await.unwrap();

        let result = framed_reader.next().await;
        assert!(result.is_err());
    }
} 
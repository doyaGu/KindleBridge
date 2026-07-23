use std::io::{self, Read, Write};

use kindlebridge_wire::{Frame, Header, HEADER_LEN};

use crate::{IoOperation, TransportConfig, TransportError, HARD_MAX_PAYLOAD};

/// Common framed-I/O boundary used by KBP sessions regardless of whether the
/// underlying byte stream is TCP, USB bulk, or a FunctionFS endpoint pair.
pub trait FrameIo {
    fn read_frame(&mut self) -> Result<Frame, TransportError>;
    fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError>;
    fn flush(&mut self) -> Result<(), TransportError>;

    /// Restore the next frame boundary after a transport-specific cancelled
    /// transfer. Byte-stream transports are already aligned; FunctionFS
    /// overrides this to scan past bytes left by an abandoned USB frame.
    fn resynchronize(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct FrameReader<R> {
    reader: R,
    config: TransportConfig,
}

impl<R: Read> FrameReader<R> {
    pub fn new(reader: R, config: TransportConfig) -> Result<Self, TransportError> {
        Ok(Self {
            reader,
            config: config.validate()?,
        })
    }

    pub fn read_frame(&mut self) -> Result<Frame, TransportError> {
        read_frame_from(&mut self.reader, self.config)
    }

    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Debug)]
pub struct FrameWriter<W> {
    writer: W,
    config: TransportConfig,
}

/// KBP framing over separate reader and writer handles.
///
/// USB bulk and FunctionFS expose one endpoint per direction rather than one
/// bidirectional socket, so they use this adapter while sharing the exact same
/// framing implementation as TCP.
#[derive(Debug)]
pub struct SplitFrameStream<R, W> {
    reader: FrameReader<R>,
    writer: FrameWriter<W>,
}

impl<R: Read, W: Write> SplitFrameStream<R, W> {
    pub fn new(reader: R, writer: W, config: TransportConfig) -> Result<Self, TransportError> {
        Ok(Self {
            reader: FrameReader::new(reader, config)?,
            writer: FrameWriter::new(writer, config)?,
        })
    }

    pub fn into_inner(self) -> (R, W) {
        (self.reader.into_inner(), self.writer.into_inner())
    }

    pub fn reader_mut(&mut self) -> &mut R {
        self.reader.get_mut()
    }

    pub fn writer_mut(&mut self) -> &mut W {
        self.writer.get_mut()
    }
}

impl<R: Read, W: Write> FrameIo for SplitFrameStream<R, W> {
    fn read_frame(&mut self) -> Result<Frame, TransportError> {
        self.reader.read_frame()
    }

    fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.writer.write_frame(frame)
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        self.writer.flush()
    }
}

impl<W: Write> FrameWriter<W> {
    pub fn new(writer: W, config: TransportConfig) -> Result<Self, TransportError> {
        Ok(Self {
            writer,
            config: config.validate()?,
        })
    }

    pub fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        write_frame_to(&mut self.writer, frame, self.config)
    }

    /// Validate and encode one complete frame for a transport-specific writer.
    pub fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, TransportError> {
        encode_frame_for_write(frame, self.config)
    }

    /// Write one frame from a contiguous header-and-payload buffer.
    ///
    /// FunctionFS uses this so its bounded write adapter chunks the complete
    /// frame rather than starting a fresh USB request boundary at the payload.
    pub fn write_frame_contiguous(&mut self, frame: &Frame) -> Result<(), TransportError> {
        let encoded = encode_frame_for_write(frame, self.config)?;
        self.writer
            .write_all(&encoded)
            .map_err(|error| TransportError::io(IoOperation::WritePayload, error))
    }

    pub fn flush(&mut self) -> Result<(), TransportError> {
        self.writer
            .flush()
            .map_err(|error| TransportError::io(IoOperation::Flush, error))
    }

    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Bidirectional framing over an arbitrary byte stream.
///
/// This type makes no authentication claim. Use [`crate::AuthenticatedFramed`]
/// at the production security boundary.
#[derive(Debug)]
pub struct FramedStream<S> {
    stream: S,
    config: TransportConfig,
}

impl<S: Read + Write> FramedStream<S> {
    pub fn new(stream: S, config: TransportConfig) -> Result<Self, TransportError> {
        Ok(Self {
            stream,
            config: config.validate()?,
        })
    }

    pub fn read_frame(&mut self) -> Result<Frame, TransportError> {
        read_frame_from(&mut self.stream, self.config)
    }

    pub fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        write_frame_to(&mut self.stream, frame, self.config)
    }

    pub fn flush(&mut self) -> Result<(), TransportError> {
        self.stream
            .flush()
            .map_err(|error| TransportError::io(IoOperation::Flush, error))
    }

    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub const fn config(&self) -> TransportConfig {
        self.config
    }

    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S: Read + Write> FrameIo for FramedStream<S> {
    fn read_frame(&mut self) -> Result<Frame, TransportError> {
        Self::read_frame(self)
    }

    fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        Self::write_frame(self, frame)
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        Self::flush(self)
    }
}

pub(crate) fn read_frame_from<R: Read>(
    reader: &mut R,
    config: TransportConfig,
) -> Result<Frame, TransportError> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    let received = read_exact_counted(reader, &mut header_bytes, IoOperation::ReadHeader)?;
    if received == 0 {
        return Err(TransportError::EndOfStream);
    }
    if received != HEADER_LEN {
        return Err(TransportError::TruncatedHeader { received });
    }

    let header = Header::decode(&header_bytes, config.limits)?;
    if header.payload_length > HARD_MAX_PAYLOAD {
        return Err(TransportError::PayloadExceedsHardLimit {
            length: header.payload_length,
            hard_limit: HARD_MAX_PAYLOAD,
        });
    }
    let payload_len = usize::try_from(header.payload_length).map_err(|_| {
        TransportError::PayloadExceedsHardLimit {
            length: header.payload_length,
            hard_limit: HARD_MAX_PAYLOAD,
        }
    })?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| TransportError::PayloadAllocation {
            length: payload_len,
        })?;
    payload.resize(payload_len, 0);
    let received = read_exact_counted(reader, &mut payload, IoOperation::ReadPayload)?;
    if received != payload_len {
        return Err(TransportError::TruncatedPayload {
            expected: payload_len,
            received,
        });
    }
    Ok(Frame { header, payload })
}

pub(crate) fn write_frame_to<W: Write>(
    writer: &mut W,
    frame: &Frame,
    config: TransportConfig,
) -> Result<(), TransportError> {
    let encoded_header = encode_header_for_write(frame, config)?;
    writer
        .write_all(&encoded_header)
        .map_err(|error| TransportError::io(IoOperation::WriteHeader, error))?;
    writer
        .write_all(&frame.payload)
        .map_err(|error| TransportError::io(IoOperation::WritePayload, error))?;
    Ok(())
}

fn encode_header_for_write(
    frame: &Frame,
    config: TransportConfig,
) -> Result<[u8; HEADER_LEN], TransportError> {
    if frame.header.payload_length > HARD_MAX_PAYLOAD {
        return Err(TransportError::PayloadExceedsHardLimit {
            length: frame.header.payload_length,
            hard_limit: HARD_MAX_PAYLOAD,
        });
    }
    let actual = frame.payload.len();
    if usize::try_from(frame.header.payload_length).ok() != Some(actual) {
        return Err(TransportError::FrameLengthMismatch {
            declared: frame.header.payload_length,
            actual,
        });
    }
    Ok(frame.header.encode(config.limits)?)
}

fn encode_frame_for_write(
    frame: &Frame,
    config: TransportConfig,
) -> Result<Vec<u8>, TransportError> {
    let header = encode_header_for_write(frame, config)?;
    let mut encoded = Vec::with_capacity(HEADER_LEN + frame.payload.len());
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&frame.payload);
    Ok(encoded)
}

fn read_exact_counted<R: Read>(
    reader: &mut R,
    buffer: &mut [u8],
    operation: IoOperation,
) -> Result<usize, TransportError> {
    let mut received = 0;
    while received < buffer.len() {
        match reader.read(&mut buffer[received..]) {
            Ok(0) => break,
            Ok(count) => received += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(TransportError::io(operation, error)),
        }
    }
    Ok(received)
}

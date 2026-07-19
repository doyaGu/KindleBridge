use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
};

use kindlebridge_wire::Frame;

use crate::{FrameIo, FramedStream, IoOperation, TransportConfig, TransportError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    Read,
    Write,
    Both,
}

impl From<ShutdownMode> for Shutdown {
    fn from(value: ShutdownMode) -> Self {
        match value {
            ShutdownMode::Read => Self::Read,
            ShutdownMode::Write => Self::Write,
            ShutdownMode::Both => Self::Both,
        }
    }
}

/// A byte stream whose implementation vouches that peer authentication and
/// confidentiality have completed before KBP framing begins.
///
/// There is deliberately no blanket implementation for [`TcpStream`]. A TLS or
/// equivalent wrapper owned by the session layer should implement this trait.
pub trait AuthenticatedStream: Read + Write + Send {
    fn shutdown(&mut self, mode: ShutdownMode) -> std::io::Result<()>;
}

/// Production framing boundary for an explicitly authenticated stream.
#[derive(Debug)]
pub struct AuthenticatedFramed<S: AuthenticatedStream> {
    framed: FramedStream<S>,
}

impl<S: AuthenticatedStream> AuthenticatedFramed<S> {
    pub fn new(stream: S, config: TransportConfig) -> Result<Self, TransportError> {
        Ok(Self {
            framed: FramedStream::new(stream, config)?,
        })
    }

    pub fn read_frame(&mut self) -> Result<Frame, TransportError> {
        self.framed.read_frame()
    }

    pub fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.framed.write_frame(frame)
    }

    pub fn flush(&mut self) -> Result<(), TransportError> {
        self.framed.flush()
    }

    pub fn shutdown(&mut self, mode: ShutdownMode) -> Result<(), TransportError> {
        self.framed
            .get_mut()
            .shutdown(mode)
            .map_err(|error| TransportError::io(IoOperation::Shutdown, error))
    }

    pub fn into_inner(self) -> S {
        self.framed.into_inner()
    }
}

/// Plain TCP KBP framing. This type does not authenticate or encrypt traffic.
#[derive(Debug)]
pub struct TcpFrameStream {
    framed: FramedStream<TcpStream>,
}

impl TcpFrameStream {
    pub fn connect(address: SocketAddr, config: TransportConfig) -> Result<Self, TransportError> {
        let config = config.validate()?;
        let stream = TcpStream::connect_timeout(&address, config.connect_timeout)
            .map_err(|error| TransportError::io(IoOperation::Connect, error))?;
        Self::from_stream(stream, config)
    }

    pub fn from_stream(stream: TcpStream, config: TransportConfig) -> Result<Self, TransportError> {
        let config = config.validate()?;
        configure_stream(&stream, config)?;
        Ok(Self {
            framed: FramedStream::new(stream, config)?,
        })
    }

    pub fn read_frame(&mut self) -> Result<Frame, TransportError> {
        self.framed.read_frame()
    }

    pub fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.framed.write_frame(frame)
    }

    pub fn flush(&mut self) -> Result<(), TransportError> {
        self.framed.flush()
    }

    pub fn shutdown(&self, mode: ShutdownMode) -> Result<(), TransportError> {
        self.framed
            .get_ref()
            .shutdown(mode.into())
            .map_err(|error| TransportError::io(IoOperation::Shutdown, error))
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.framed
            .get_ref()
            .local_addr()
            .map_err(|error| TransportError::io(IoOperation::Address, error))
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, TransportError> {
        self.framed
            .get_ref()
            .peer_addr()
            .map_err(|error| TransportError::io(IoOperation::Address, error))
    }

    /// Clones the underlying socket so one thread can read while another writes.
    /// The clone retains the same bounded framing and timeout configuration.
    pub fn try_clone(&self) -> Result<Self, TransportError> {
        let stream = self
            .framed
            .get_ref()
            .try_clone()
            .map_err(|error| TransportError::io(IoOperation::Configure, error))?;
        Self::from_stream(stream, self.framed.config())
    }

    pub fn into_inner(self) -> TcpStream {
        self.framed.into_inner()
    }
}

impl FrameIo for TcpFrameStream {
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

/// Plain TCP listener companion to [`TcpFrameStream`]. Authentication remains
/// the caller's responsibility.
#[derive(Debug)]
pub struct TcpFrameListener {
    listener: TcpListener,
    config: TransportConfig,
}

impl TcpFrameListener {
    pub fn bind(address: SocketAddr, config: TransportConfig) -> Result<Self, TransportError> {
        let config = config.validate()?;
        let listener = TcpListener::bind(address)
            .map_err(|error| TransportError::io(IoOperation::Bind, error))?;
        Ok(Self { listener, config })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.listener
            .local_addr()
            .map_err(|error| TransportError::io(IoOperation::Address, error))
    }

    pub fn accept(&self) -> Result<(TcpFrameStream, SocketAddr), TransportError> {
        let (stream, address) = self
            .listener
            .accept()
            .map_err(|error| TransportError::io(IoOperation::Accept, error))?;
        Ok((TcpFrameStream::from_stream(stream, self.config)?, address))
    }
}

fn configure_stream(stream: &TcpStream, config: TransportConfig) -> Result<(), TransportError> {
    stream
        .set_read_timeout(config.read_timeout)
        .and_then(|()| stream.set_write_timeout(config.write_timeout))
        .and_then(|()| stream.set_nodelay(config.nodelay))
        .map_err(|error| TransportError::io(IoOperation::Configure, error))
}

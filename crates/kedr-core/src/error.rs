// SPDX-License-Identifier: Apache-2.0
use std::{fmt, io};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Open,
    Read,
    Write,
    Sync,
    Resize,
    Length,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Open => "open",
            Self::Read => "read",
            Self::Write => "write",
            Self::Sync => "sync",
            Self::Resize => "resize",
            Self::Length => "length",
        };
        f.write_str(name)
    }
}

/// Diagnostic identifiers are stable; OS-specific error wording is not.
#[derive(Debug)]
pub enum Error {
    Io {
        operation: Operation,
        source: io::Error,
    },
    InvalidRange {
        offset: u64,
        length: usize,
    },
    UnexpectedEof {
        offset: u64,
        expected: usize,
        actual: usize,
    },
    WriteZero {
        offset: u64,
    },
    LimitExceeded {
        limit: usize,
        requested: u64,
    },
    Allocation {
        requested: usize,
    },
    InvalidBackend {
        operation: Operation,
        reported: usize,
        requested: usize,
    },
    InvalidFault {
        operation: Operation,
    },
    InvalidArgument {
        message: &'static str,
    },
    CorruptPage {
        reason: &'static str,
    },
    PageFull,
    RecordTooLarge {
        length: usize,
        limit: usize,
    },
    InvalidSlot {
        slot: u16,
        count: u16,
    },
}

impl Error {
    pub fn io(operation: Operation, source: io::Error) -> Self {
        Self::Io { operation, source }
    }

    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "KD1001",
            Self::InvalidRange { .. } => "KD1002",
            Self::UnexpectedEof { .. } => "KD1003",
            Self::WriteZero { .. } => "KD1004",
            Self::LimitExceeded { .. } => "KD1005",
            Self::Allocation { .. } => "KD1006",
            Self::InvalidBackend { .. } => "KD1007",
            Self::InvalidFault { .. } => "KD1008",
            Self::InvalidArgument { .. } => "KD1009",
            Self::CorruptPage { .. } => "KD2001",
            Self::PageFull => "KD2002",
            Self::RecordTooLarge { .. } => "KD2003",
            Self::InvalidSlot { .. } => "KD2004",
        }
    }

    pub fn is_interrupted(&self) -> bool {
        matches!(self, Self::Io { source, .. } if source.kind() == io::ErrorKind::Interrupted)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.code())?;
        match self {
            Self::Io { operation, source } => write!(f, "{operation} failed: {source}"),
            Self::InvalidRange { offset, length } => {
                write!(f, "invalid byte range at {offset} with length {length}")
            }
            Self::UnexpectedEof {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "short read at {offset}: expected {expected} bytes, received {actual}"
            ),
            Self::WriteZero { offset } => write!(f, "write made no progress at {offset}"),
            Self::LimitExceeded { limit, requested } => {
                write!(f, "storage limit {limit} bytes exceeded by {requested}")
            }
            Self::Allocation { requested } => write!(f, "cannot allocate {requested} bytes"),
            Self::InvalidBackend {
                operation,
                reported,
                requested,
            } => write!(
                f,
                "{operation} backend reported {reported} bytes for a {requested}-byte request"
            ),
            Self::InvalidFault { operation } => {
                write!(f, "fault action is incompatible with {operation}")
            }
            Self::InvalidArgument { message } => f.write_str(message),
            Self::CorruptPage { reason } => write!(f, "invalid page: {reason}"),
            Self::PageFull => f.write_str("page has insufficient contiguous free space"),
            Self::RecordTooLarge { length, limit } => {
                write!(f, "record length {length} exceeds {limit}")
            }
            Self::InvalidSlot { slot, count } => {
                write!(f, "slot {slot} is outside slot count {count}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

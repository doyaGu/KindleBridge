//! Security-sensitive core for the KindleBridge Bundle (KBB)
//! `kindlebridge.bundle.v1` profile.
//!
//! The crate deliberately implements only the KindleBridge 1.0 profile. Generic
//! multi-target bundles, dependencies, migrations, distributor signatures, and
//! repository metadata are outside this crate's accepted profile.

mod activation;
#[cfg(feature = "full")]
mod builder;
mod cbor;
mod error;
#[cfg(feature = "full")]
mod header;
mod install;
mod model;
mod path;
#[cfg(feature = "full")]
mod verify;

pub use activation::{ActivationEntry, ActivationGeneration, GenerationId};
#[cfg(feature = "full")]
pub use builder::{BuildConfig, BundleBuilder, CompressionPolicy};
pub use error::{Error, ErrorCode, Result};
#[cfg(feature = "full")]
pub use header::{Header, FORMAT_MAJOR, FORMAT_MINOR, HEADER_SIZE, MAGIC};
pub use install::{
    BlockStatus, CommitOutcome, InstallStore, RecoveryAction, RecoveryReport, StagedGeneration,
};
pub use model::{
    BlockDescriptor, BlockRef, BundleKind, DataPolicy, DataPolicyKind, Digest, Envelope, FileEntry,
    FileType, Permissions, ProcessPolicy, Publisher, RestartPolicy, RotationProof,
    RotationProofSignedData, SignatureEntry, SignaturePolicy, Tree, Variant,
};
pub use path::{validate_bundle_path, validate_symlink_target};
#[cfg(feature = "full")]
pub use verify::{
    inspect, inspect_bytes, verify, verify_bytes, Inspection, VerifiedBundle, VerifyOptions,
};

/// The only profile accepted by the KindleBridge 1.0 implementation.
pub const PROFILE: &str = "kindlebridge.bundle.v1";
/// Fixed per-file chunk size for the profile.
pub const BLOCK_SIZE: usize = 65_536;

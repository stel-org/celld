//! Preparation boundary for JavaScript artifacts produced by Base Worker.
//!
//! Base Worker remains the source processor. This module invokes its selected
//! executable with the fork's existing bundler and returns only the generated
//! ES module source plus provenance needed by celld's existing V8 host.

use sha2::{Digest, Sha256};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The deliberately bounded source-resolution policy used for one artifact.
///
/// `LocalOnly` preserves the B3.1.2 profile: imports must resolve from the
/// selected local source graph. `JsrLocked` permits only JSR imports and
/// requires an existing Deno-compatible lockfile. celld does not resolve or
/// cache either kind of dependency; Base Worker does so in its child process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BaseWorkerPreparationMode {
    /// Local JavaScript or TypeScript source with no remote or npm imports.
    LocalOnly,
    /// JSR imports pinned by an existing Deno lockfile.
    JsrLocked { lockfile: PathBuf },
}

/// A prepared ES module ready to become `WorkerConfigOptions::src`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseWorkerArtifact {
    entrypoint: PathBuf,
    executable: PathBuf,
    preparation_mode: BaseWorkerPreparationMode,
    source: String,
    sha256: String,
    byte_len: usize,
}

impl BaseWorkerArtifact {
    /// The canonical source entrypoint Base Worker prepared.
    pub fn entrypoint(&self) -> &Path {
        &self.entrypoint
    }

    /// The explicit Base Worker executable that produced this artifact.
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// The bounded source-resolution policy used to prepare this artifact.
    pub fn preparation_mode(&self) -> &BaseWorkerPreparationMode {
        &self.preparation_mode
    }

    /// The generated JavaScript ES module for celld's existing Worker config.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// SHA-256 of the generated UTF-8 JavaScript bytes.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Size of the generated JavaScript bytes.
    pub fn byte_len(&self) -> usize {
        self.byte_len
    }

    /// Consume the artifact when transferring its source to a Worker config.
    pub fn into_source(self) -> String {
        self.source
    }
}

/// Explicit failures at the Base Worker source-preparation boundary.
#[derive(Debug)]
pub enum BaseWorkerArtifactError {
    ExecutablePathNotAbsolute {
        executable: PathBuf,
    },
    ExecutableUnavailable {
        executable: PathBuf,
        reason: String,
    },
    EntrypointPathNotAbsolute {
        entrypoint: PathBuf,
    },
    EntrypointUnavailable {
        entrypoint: PathBuf,
        reason: String,
    },
    LockfilePathNotAbsolute {
        lockfile: PathBuf,
    },
    LockfileUnavailable {
        lockfile: PathBuf,
        reason: String,
    },
    TemporaryArtifactDirectory {
        reason: String,
    },
    PreparationFailed {
        executable: PathBuf,
        entrypoint: PathBuf,
        status: Option<i32>,
        diagnostics: String,
    },
    ArtifactUnreadable {
        entrypoint: PathBuf,
        reason: String,
    },
    ArtifactEmpty {
        entrypoint: PathBuf,
    },
    ArtifactNotUtf8 {
        entrypoint: PathBuf,
        reason: String,
    },
}

impl fmt::Display for BaseWorkerArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutablePathNotAbsolute { executable } => write!(
                formatter,
                "Base Worker executable path must be absolute: {}",
                executable.display()
            ),
            Self::ExecutableUnavailable { executable, reason } => write!(
                formatter,
                "Base Worker executable unavailable at {}: {reason}",
                executable.display()
            ),
            Self::EntrypointPathNotAbsolute { entrypoint } => write!(
                formatter,
                "Base Worker entrypoint path must be absolute: {}",
                entrypoint.display()
            ),
            Self::EntrypointUnavailable { entrypoint, reason } => write!(
                formatter,
                "Base Worker entrypoint unavailable at {}: {reason}",
                entrypoint.display()
            ),
            Self::LockfilePathNotAbsolute { lockfile } => write!(
                formatter,
                "Base Worker JSR lockfile path must be absolute: {}",
                lockfile.display()
            ),
            Self::LockfileUnavailable { lockfile, reason } => write!(
                formatter,
                "Base Worker JSR lockfile unavailable at {}: {reason}",
                lockfile.display()
            ),
            Self::TemporaryArtifactDirectory { reason } => {
                write!(formatter, "create Base Worker artifact directory: {reason}")
            }
            Self::PreparationFailed {
                executable,
                entrypoint,
                status,
                diagnostics,
            } => write!(
                formatter,
                "Base Worker preparation failed for {} using {} (status {}): {diagnostics}",
                entrypoint.display(),
                executable.display(),
                status.map_or_else(
                    || "terminated by signal".to_string(),
                    |code| code.to_string()
                )
            ),
            Self::ArtifactUnreadable { entrypoint, reason } => write!(
                formatter,
                "read generated Base Worker artifact for {}: {reason}",
                entrypoint.display()
            ),
            Self::ArtifactEmpty { entrypoint } => write!(
                formatter,
                "Base Worker generated an empty artifact for {}",
                entrypoint.display()
            ),
            Self::ArtifactNotUtf8 { entrypoint, reason } => write!(
                formatter,
                "Base Worker generated non-UTF-8 JavaScript for {}: {reason}",
                entrypoint.display()
            ),
        }
    }
}

impl std::error::Error for BaseWorkerArtifactError {}

/// Invokes one explicitly selected Base Worker fork executable.
///
/// This type never searches `PATH`, never selects a system `deno`, and keeps
/// no artifact cache. Every preparation call runs the selected entrypoint
/// through Base Worker's existing bundler and returns either that new result or
/// a preparation error.
#[derive(Clone, Debug)]
pub struct BaseWorkerArtifactPreparer {
    executable: PathBuf,
}

impl BaseWorkerArtifactPreparer {
    /// Select an absolute executable belonging to the intended Base Worker fork.
    #[allow(
        clippy::disallowed_methods,
        reason = "This validates an explicit external source-processor executable, not celld node storage."
    )]
    pub fn new(executable: impl Into<PathBuf>) -> Result<Self, BaseWorkerArtifactError> {
        let executable = executable.into();
        if !executable.is_absolute() {
            return Err(BaseWorkerArtifactError::ExecutablePathNotAbsolute { executable });
        }
        let metadata = std::fs::metadata(&executable).map_err(|error| {
            BaseWorkerArtifactError::ExecutableUnavailable {
                executable: executable.clone(),
                reason: error.to_string(),
            }
        })?;
        if !metadata.is_file() {
            return Err(BaseWorkerArtifactError::ExecutableUnavailable {
                executable,
                reason: "path is not a regular file".to_string(),
            });
        }
        Ok(Self { executable })
    }

    /// Run Base Worker's browser ESM bundler for one selected local entrypoint.
    #[allow(
        clippy::disallowed_methods,
        reason = "This intentionally reads an external source artifact after the selected Base Worker process creates it."
    )]
    pub fn prepare(
        &self,
        entrypoint: impl Into<PathBuf>,
    ) -> Result<BaseWorkerArtifact, BaseWorkerArtifactError> {
        self.prepare_with_mode(entrypoint, BaseWorkerPreparationMode::LocalOnly)
    }

    /// Run Base Worker's browser ESM bundler under an explicit resolution mode.
    ///
    /// `LocalOnly` keeps B3.1.2's remote-free behavior. `JsrLocked` delegates
    /// the selected JSR dependency graph and cache behavior to Base Worker's
    /// existing Deno implementation while requiring an explicit lockfile.
    #[allow(
        clippy::disallowed_methods,
        reason = "This intentionally reads an external source artifact after the selected Base Worker process creates it."
    )]
    pub fn prepare_with_mode(
        &self,
        entrypoint: impl Into<PathBuf>,
        mode: BaseWorkerPreparationMode,
    ) -> Result<BaseWorkerArtifact, BaseWorkerArtifactError> {
        let entrypoint = entrypoint.into();
        if !entrypoint.is_absolute() {
            return Err(BaseWorkerArtifactError::EntrypointPathNotAbsolute { entrypoint });
        }
        let entrypoint = entrypoint.canonicalize().map_err(|error| {
            BaseWorkerArtifactError::EntrypointUnavailable {
                entrypoint: entrypoint.clone(),
                reason: error.to_string(),
            }
        })?;
        let mode = match mode {
            BaseWorkerPreparationMode::LocalOnly => BaseWorkerPreparationMode::LocalOnly,
            BaseWorkerPreparationMode::JsrLocked { lockfile } => {
                if !lockfile.is_absolute() {
                    return Err(BaseWorkerArtifactError::LockfilePathNotAbsolute { lockfile });
                }
                let lockfile = lockfile.canonicalize().map_err(|error| {
                    BaseWorkerArtifactError::LockfileUnavailable {
                        lockfile: lockfile.clone(),
                        reason: error.to_string(),
                    }
                })?;
                let metadata = std::fs::metadata(&lockfile).map_err(|error| {
                    BaseWorkerArtifactError::LockfileUnavailable {
                        lockfile: lockfile.clone(),
                        reason: error.to_string(),
                    }
                })?;
                if !metadata.is_file() {
                    return Err(BaseWorkerArtifactError::LockfileUnavailable {
                        lockfile,
                        reason: "path is not a regular file".to_string(),
                    });
                }
                BaseWorkerPreparationMode::JsrLocked { lockfile }
            }
        };
        let temporary_directory = tempfile::Builder::new()
            .prefix("celld-base-worker-artifact-")
            .tempdir()
            .map_err(
                |error| BaseWorkerArtifactError::TemporaryArtifactDirectory {
                    reason: error.to_string(),
                },
            )?;
        let artifact_path = temporary_directory.path().join("worker.js");
        let mut command = Command::new(&self.executable);
        command.args(["bundle", "--no-config", "--no-npm"]);
        match &mode {
            BaseWorkerPreparationMode::LocalOnly => {
                command.args(["--no-lock", "--no-remote"]);
            }
            BaseWorkerPreparationMode::JsrLocked { lockfile } => {
                command
                    .args(["--allow-import=jsr.io", "--lock"])
                    .arg(lockfile)
                    .arg("--frozen-lockfile");
            }
        }
        let output = command
            .args(["--platform=browser", "--format=esm", "-o"])
            .arg(&artifact_path)
            .arg(&entrypoint)
            .output()
            .map_err(|error| BaseWorkerArtifactError::ExecutableUnavailable {
                executable: self.executable.clone(),
                reason: error.to_string(),
            })?;
        if !output.status.success() {
            let diagnostics = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let diagnostics = if diagnostics.is_empty() {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            } else {
                diagnostics
            };
            return Err(BaseWorkerArtifactError::PreparationFailed {
                executable: self.executable.clone(),
                entrypoint,
                status: output.status.code(),
                diagnostics,
            });
        }
        let bytes = std::fs::read(&artifact_path).map_err(|error| {
            BaseWorkerArtifactError::ArtifactUnreadable {
                entrypoint: entrypoint.clone(),
                reason: error.to_string(),
            }
        })?;
        if bytes.is_empty() {
            return Err(BaseWorkerArtifactError::ArtifactEmpty { entrypoint });
        }
        let byte_len = bytes.len();
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let source =
            String::from_utf8(bytes).map_err(|error| BaseWorkerArtifactError::ArtifactNotUtf8 {
                entrypoint: entrypoint.clone(),
                reason: error.to_string(),
            })?;
        Ok(BaseWorkerArtifact {
            entrypoint,
            executable: self.executable.clone(),
            preparation_mode: mode,
            source,
            sha256,
            byte_len,
        })
    }
}

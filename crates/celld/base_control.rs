//! A deliberately narrow, local integration probe for a paired Base install.
//!
//! This is not celld's operator HTTP surface and it does not perform any cell
//! operation.  When enabled, it binds a Unix-domain socket in an explicitly
//! configured private directory and answers one authenticated readiness probe.

use crate::actor::AppHandle;
use anyhow::{bail, Context};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

const CONTROL_DIRECTORY: &str = "CELLD_BASE_CONTROL_DIR";
const SOCKET_NAME: &str = "base-control.sock";
const INSTALLATION_ID_NAME: &str = "base-installation-id";
const SECRET_NAME: &str = "base-control.secret";
const PROTOCOL: &str = "base-celld";
const PROTOCOL_VERSION: u32 = 1;
const MAX_MESSAGE_BYTES: usize = 4 * 1024;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

type HmacSha256 = Hmac<Sha256>;

/// The active local control server. Dropping it stops accepting probes and
/// removes its socket path; identity and pairing material remain durable.
pub struct BaseControlServer {
    socket_path: PathBuf,
    task: JoinHandle<()>,
}

impl Drop for BaseControlServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = fs::remove_file(&self.socket_path);
    }
}

struct ControlDirectory {
    socket_path: PathBuf,
    installation_id: String,
    secret: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProbeRequest {
    protocol: String,
    protocol_version: u32,
    nonce: String,
    proof: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeResponse<'a> {
    protocol: &'static str,
    protocol_version: u32,
    installation_id: &'a str,
    implementation: &'static str,
    implementation_version: &'static str,
    readiness: &'static str,
    nonce: &'a str,
    proof: String,
}

#[derive(Serialize)]
struct Rejection<'a> {
    error: &'a str,
}

/// Starts the optional Base integration boundary selected by
/// `CELLD_BASE_CONTROL_DIR`. A normal celld installation remains independent
/// of Base when this variable is absent.
pub async fn start_from_environment(app: AppHandle) -> anyhow::Result<Option<BaseControlServer>> {
    let Some(directory) = std::env::var_os(CONTROL_DIRECTORY) else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    let control = ControlDirectory::prepare(&directory)?;
    let listener = bind_socket(&control.socket_path)?;
    let socket_path = control.socket_path.clone();
    let task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((stream, _)) = accepted else {
                return;
            };
            // The control surface intentionally services one short request at
            // a time. It has no unbounded worker pool and a stalled peer is
            // bounded before another local probe can be accepted.
            let _ = tokio::time::timeout(
                REQUEST_TIMEOUT,
                handle_probe(
                    stream,
                    app.clone(),
                    &control.installation_id,
                    &control.secret,
                ),
            )
            .await;
        }
    });
    tracing::info!(
        event = "base_control_ready",
        socket = %socket_path.display(),
        "Base local control probe is ready"
    );
    Ok(Some(BaseControlServer { socket_path, task }))
}

impl ControlDirectory {
    fn prepare(directory: &Path) -> anyhow::Result<Self> {
        if !directory.is_absolute() {
            bail!("{CONTROL_DIRECTORY} must be an absolute directory");
        }
        if directory.exists() {
            let metadata = fs::symlink_metadata(directory).with_context(|| {
                format!("inspect Base control directory {}", directory.display())
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("{CONTROL_DIRECTORY} must name a real directory");
            }
        } else {
            fs::create_dir_all(directory).with_context(|| {
                format!("create Base control directory {}", directory.display())
            })?;
        }
        set_mode(directory, 0o700)?;

        let installation_id =
            read_or_create_installation_id(&directory.join(INSTALLATION_ID_NAME))?;
        let secret = read_or_create_secret(&directory.join(SECRET_NAME))?;
        Ok(Self {
            socket_path: directory.join(SOCKET_NAME),
            installation_id,
            secret,
        })
    }
}

fn bind_socket(path: &Path) -> anyhow::Result<UnixListener> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect Base control socket {}", path.display()))?;
        if !metadata.file_type().is_socket() {
            bail!(
                "refusing to replace non-socket Base control path {}",
                path.display()
            );
        }
        fs::remove_file(path)
            .with_context(|| format!("remove stale Base control socket {}", path.display()))?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("bind Base control socket {}", path.display()))?;
    set_mode(path, 0o600)?;
    Ok(listener)
}

fn read_or_create_installation_id(path: &Path) -> anyhow::Result<String> {
    if path.exists() {
        let id = read_private_text(path)?;
        validate_installation_id(&id)?;
        return Ok(id);
    }
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).context("generate Base control installation identity")?;
    let hex = hex::encode(random);
    let id = format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    );
    write_private_text(path, &id)?;
    Ok(id)
}

fn read_or_create_secret(path: &Path) -> anyhow::Result<Vec<u8>> {
    if path.exists() {
        let encoded = read_private_text(path)?;
        let secret = hex::decode(encoded).context("decode Base control secret")?;
        if secret.len() != 32 {
            bail!("Base control secret must contain exactly 32 bytes");
        }
        return Ok(secret);
    }
    let mut secret = [0_u8; 32];
    getrandom::fill(&mut secret).context("generate Base control secret")?;
    write_private_text(path, &hex::encode(secret))?;
    Ok(secret.to_vec())
}

fn read_private_text(path: &Path) -> anyhow::Result<String> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect private Base control file {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "Base control file {} must be a regular file",
            path.display()
        );
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "Base control file {} is accessible to other users",
            path.display()
        );
    }
    fs::read_to_string(path)
        .with_context(|| format!("read private Base control file {}", path.display()))
        .map(|value| value.trim().to_owned())
}

fn write_private_text(path: &Path, value: &str) -> anyhow::Result<()> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(file) => file,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create private Base control file {}", path.display()))
        }
    };
    file.write_all(value.as_bytes())
        .with_context(|| format!("write private Base control file {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("finish private Base control file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync private Base control file {}", path.display()))?;
    Ok(())
}

fn set_mode(path: &Path, mode: u32) -> anyhow::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set private mode on {}", path.display()))
}

fn validate_installation_id(id: &str) -> anyhow::Result<()> {
    if id.len() != 36
        || id.as_bytes().iter().enumerate().any(|(index, byte)| {
            !matches!(index, 8 | 13 | 18 | 23) && !byte.is_ascii_hexdigit()
                || matches!(index, 8 | 13 | 18 | 23) && *byte != b'-'
        })
    {
        bail!("Base control installation identity is malformed");
    }
    Ok(())
}

async fn handle_probe(
    mut stream: UnixStream,
    app: AppHandle,
    installation_id: &str,
    secret: &[u8],
) -> anyhow::Result<()> {
    let body = read_message(&mut stream).await?;
    let request: ProbeRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };
    if request.protocol != PROTOCOL
        || request.protocol_version != PROTOCOL_VERSION
        || !valid_nonce(&request.nonce)
        || !verify(secret, &request_material(&request.nonce), &request.proof)
    {
        write_json(
            &mut stream,
            &Rejection {
                error: "authentication_rejected",
            },
        )
        .await?;
        return Ok(());
    }

    let readiness = if !app.is_draining() && app.fleet_ready() && app.healthy().await {
        "ready"
    } else {
        "not_ready"
    };
    let response = ProbeResponse {
        protocol: PROTOCOL,
        protocol_version: PROTOCOL_VERSION,
        installation_id,
        implementation: "celld",
        implementation_version: env!("CARGO_PKG_VERSION"),
        readiness,
        nonce: &request.nonce,
        proof: sign(
            secret,
            &response_material(
                installation_id,
                env!("CARGO_PKG_VERSION"),
                readiness,
                &request.nonce,
            ),
        ),
    };
    write_json(&mut stream, &response).await
}

async fn read_message(stream: &mut UnixStream) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::with_capacity(256);
    loop {
        let mut byte = [0_u8; 1];
        let count = stream
            .read(&mut byte)
            .await
            .context("read Base control request")?;
        if count == 0 {
            bail!("Base control peer closed without a request");
        }
        if byte[0] == b'\n' {
            return Ok(body);
        }
        if body.len() == MAX_MESSAGE_BYTES {
            bail!("Base control request exceeds its maximum size");
        }
        body.push(byte[0]);
    }
}

async fn write_json(stream: &mut UnixStream, value: &impl Serialize) -> anyhow::Result<()> {
    let mut body = serde_json::to_vec(value).context("encode Base control response")?;
    body.push(b'\n');
    stream
        .write_all(&body)
        .await
        .context("write Base control response")?;
    stream.flush().await.context("flush Base control response")
}

fn valid_nonce(nonce: &str) -> bool {
    nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn request_material(nonce: &str) -> String {
    format!("request:{PROTOCOL}:{PROTOCOL_VERSION}:{nonce}")
}

fn response_material(
    installation_id: &str,
    implementation_version: &str,
    readiness: &str,
    nonce: &str,
) -> String {
    format!(
        "response:{PROTOCOL}:{PROTOCOL_VERSION}:{installation_id}:celld:{implementation_version}:{readiness}:{nonce}"
    )
}

fn sign(secret: &[u8], material: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts every key length");
    mac.update(material.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify(secret: &[u8], material: &str, proof: &str) -> bool {
    let Ok(bytes) = hex::decode(proof) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(material.as_bytes());
    mac.verify_slice(&bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_proof_is_bound_to_its_nonce() {
        let secret = [7_u8; 32];
        let proof = sign(
            &secret,
            &request_material("0123456789abcdef0123456789abcdef"),
        );
        assert!(verify(
            &secret,
            &request_material("0123456789abcdef0123456789abcdef"),
            &proof
        ));
        assert!(!verify(
            &secret,
            &request_material("fedcba9876543210fedcba9876543210"),
            &proof
        ));
    }

    #[test]
    fn installation_identity_is_strictly_shaped() {
        assert!(validate_installation_id("01234567-89ab-cdef-0123-456789abcdef").is_ok());
        assert!(validate_installation_id("not-an-installation-id").is_err());
    }
}

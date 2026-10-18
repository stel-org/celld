//! A deliberately narrow, local integration probe for a paired Base install.
//!
//! This is not celld's operator HTTP surface. When enabled, it binds a
//! Unix-domain socket in an explicitly configured private directory and
//! exposes only paired Base readiness, fixed diagnostic lifecycle, and
//! non-activating presence operations.

// This module is an operator-selected host-side UDS boundary: it creates and
// protects pairing files, owns a listener, and applies request deadlines. It
// is deliberately outside celld's injected node-storage and actor-effect
// paths, so its host filesystem, clock, and task primitives are explicit.
#![allow(clippy::disallowed_methods)]

use crate::actor::{AppHandle, LocalRequestFailure, Routed};
use crate::engine_api::{RequestBody, RuntimeFetch};
use anyhow::{bail, Context};
use bytes::Bytes;
use celld_logic::Route;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const CONTROL_DIRECTORY: &str = "CELLD_BASE_CONTROL_DIR";
const SOCKET_NAME: &str = "base-control.sock";
const INSTALLATION_ID_NAME: &str = "base-installation-id";
const SECRET_NAME: &str = "base-control.secret";
const PROTOCOL: &str = "base-celld";
const PROTOCOL_VERSION: u32 = 1;
const OBSERVATION_PROTOCOL_VERSION: u32 = 2;
const MAX_MESSAGE_BYTES: usize = 4 * 1024;
const MAX_CONCURRENT_CONNECTIONS: usize = 8;
const SHORT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const EXECUTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const BUSY_RESPONSE_TIMEOUT: Duration = Duration::from_millis(100);
const REPLAY_CACHE_CAPACITY: usize = 1024;
const REPLAY_WINDOW: Duration = Duration::from_secs(60);
const DIAGNOSTIC_CLASS: &str = "BaseDiagnostic";
const MAX_DIAGNOSTIC_RESPONSE_BYTES: usize = 1024;

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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutionRequest {
    operation: String,
    protocol: String,
    protocol_version: u32,
    cell_id: String,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionResponse<'a> {
    operation: &'static str,
    protocol: &'static str,
    protocol_version: u32,
    installation_id: &'a str,
    implementation: &'static str,
    implementation_version: &'static str,
    cell_id: &'a str,
    scope: &'a str,
    outcome: &'static str,
    status: Option<u16>,
    body: Option<&'a str>,
    nonce: &'a str,
    proof: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InspectionResponse<'a> {
    operation: &'static str,
    protocol: &'static str,
    protocol_version: u32,
    installation_id: &'a str,
    implementation: &'static str,
    implementation_version: &'static str,
    process_instance_id: &'a str,
    cell_id: &'a str,
    scope: &'a str,
    local_presence: &'static str,
    epoch: Option<u64>,
    observed_at_ms: u128,
    nonce: &'a str,
    proof: String,
}

#[derive(Default)]
struct ReplayGuard {
    entries: Mutex<VecDeque<(Instant, String)>>,
}

impl ReplayGuard {
    fn record_if_new(&self, identity: String) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        let now = Instant::now();
        while entries
            .front()
            .is_some_and(|(recorded, _)| now.duration_since(*recorded) > REPLAY_WINDOW)
        {
            entries.pop_front();
        }
        if entries.iter().any(|(_, recorded)| recorded == &identity) {
            return false;
        }
        if entries.len() == REPLAY_CACHE_CAPACITY {
            entries.pop_front();
        }
        entries.push_back((now, identity));
        true
    }
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
    let process_instance_id = random_process_instance_id()?;
    let admission = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    let replay_guard = Arc::new(ReplayGuard::default());
    let task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((stream, _)) = accepted else {
                return;
            };
            let Ok(permit) = admission.clone().try_acquire_owned() else {
                // This bounded write deliberately happens in the accept loop:
                // spawning a task for every rejected peer would reintroduce an
                // unbounded local work queue. A stalled reject is capped.
                let _ = tokio::time::timeout(BUSY_RESPONSE_TIMEOUT, reject_busy(stream)).await;
                continue;
            };
            let app = app.clone();
            let installation_id = control.installation_id.clone();
            let secret = control.secret.clone();
            let process_instance_id = process_instance_id.clone();
            let replay_guard = replay_guard.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = handle_control(
                    stream,
                    app,
                    &installation_id,
                    &secret,
                    &process_instance_id,
                    replay_guard,
                )
                .await;
            });
        }
    });
    tracing::info!(
        event = "base_control_ready",
        socket = %socket_path.display(),
        "Base local control probe is ready"
    );
    Ok(Some(BaseControlServer { socket_path, task }))
}

fn random_process_instance_id() -> anyhow::Result<String> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).context("generate Base control process identity")?;
    let hex = hex::encode(random);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
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

async fn handle_control(
    mut stream: UnixStream,
    app: AppHandle,
    installation_id: &str,
    secret: &[u8],
    process_instance_id: &str,
    replay_guard: Arc<ReplayGuard>,
) -> anyhow::Result<()> {
    let body = tokio::time::timeout(SHORT_REQUEST_TIMEOUT, read_message(&mut stream))
        .await
        .context("Base control request body timed out")??;
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    if value.get("operation").is_some() {
        return handle_execution(
            stream,
            app,
            installation_id,
            secret,
            process_instance_id,
            replay_guard,
            value,
        )
        .await;
    }
    let request: ProbeRequest = match serde_json::from_value(value) {
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

async fn handle_execution(
    mut stream: UnixStream,
    app: AppHandle,
    installation_id: &str,
    secret: &[u8],
    process_instance_id: &str,
    replay_guard: Arc<ReplayGuard>,
    value: serde_json::Value,
) -> anyhow::Result<()> {
    let request: ExecutionRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };
    let supported = matches!(
        (request.operation.as_str(), request.protocol_version),
        ("executeDiagnostic" | "evictDiagnostic", PROTOCOL_VERSION)
            | ("inspectDiagnostic", OBSERVATION_PROTOCOL_VERSION)
    );
    if !supported
        || request.protocol != PROTOCOL
        || !valid_nonce(&request.nonce)
        || !valid_cell_id(&request.cell_id)
        || !verify(
            secret,
            &execution_request_material(
                request.protocol_version,
                &request.operation,
                &request.cell_id,
                &request.nonce,
            ),
            &request.proof,
        )
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

    let scope = format!("{DIAGNOSTIC_CLASS}:{}", request.cell_id);
    if request.operation == "inspectDiagnostic" {
        return tokio::time::timeout(
            SHORT_REQUEST_TIMEOUT,
            inspect_diagnostic(
                stream,
                app,
                installation_id,
                secret,
                process_instance_id,
                request,
                scope,
            ),
        )
        .await
        .context("Base diagnostic inspection timed out")?;
    }
    if !replay_guard.record_if_new(format!(
        "{}:{}:{}",
        request.operation, request.cell_id, request.nonce
    )) {
        write_json(
            &mut stream,
            &Rejection {
                error: "replay_rejected",
            },
        )
        .await?;
        return Ok(());
    }
    tokio::time::timeout(
        EXECUTION_REQUEST_TIMEOUT,
        execute_or_evict(stream, app, installation_id, secret, request, scope),
    )
    .await
    .context("Base diagnostic lifecycle operation timed out")?
}

async fn execute_or_evict(
    mut stream: UnixStream,
    app: AppHandle,
    installation_id: &str,
    secret: &[u8],
    request: ExecutionRequest,
    scope: String,
) -> anyhow::Result<()> {
    let (outcome, status, body) = match request.operation.as_str() {
        "executeDiagnostic" => match execute_diagnostic(&app, scope.clone()).await {
            Ok((status, body)) => ("executed", Some(status), Some(body)),
            Err(error) => {
                tracing::warn!(%scope, error = %error, "Base diagnostic execution refused");
                ("execution_refused", None, None)
            }
        },
        "evictDiagnostic" => match app.evict(scope.clone()).await {
            Ok(celld_logic::EvictSuccess::Evicted) => ("evicted", None, None),
            Ok(celld_logic::EvictSuccess::AlreadyAbsent) => ("already_absent", None, None),
            Err(error) => {
                tracing::warn!(%scope, kind = error.kind(), reason = error.reason(), "Base diagnostic eviction refused");
                ("eviction_refused", None, None)
            }
        },
        _ => unreachable!("operation was validated"),
    };
    let response = ExecutionResponse {
        operation: match request.operation.as_str() {
            "executeDiagnostic" => "executeDiagnostic",
            "evictDiagnostic" => "evictDiagnostic",
            _ => unreachable!("operation was validated"),
        },
        protocol: PROTOCOL,
        protocol_version: PROTOCOL_VERSION,
        installation_id,
        implementation: "celld",
        implementation_version: env!("CARGO_PKG_VERSION"),
        cell_id: &request.cell_id,
        scope: &scope,
        outcome,
        status,
        body: body.as_deref(),
        nonce: &request.nonce,
        proof: sign(
            secret,
            &execution_response_material(
                &request.operation,
                installation_id,
                env!("CARGO_PKG_VERSION"),
                &request.cell_id,
                &scope,
                outcome,
                status,
                body.as_deref(),
                &request.nonce,
            ),
        ),
    };
    write_json(&mut stream, &response).await
}

async fn inspect_diagnostic(
    mut stream: UnixStream,
    app: AppHandle,
    installation_id: &str,
    secret: &[u8],
    process_instance_id: &str,
    request: ExecutionRequest,
    scope: String,
) -> anyhow::Result<()> {
    let presence = app.presence().await.context("celld actor is unavailable")?;
    let epoch = presence
        .cells
        .iter()
        .find(|cell| cell.id == scope)
        .map(|cell| cell.epoch);
    let local_presence = if epoch.is_some() {
        "locally_present"
    } else {
        "locally_absent"
    };
    let observed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time precedes Unix epoch")?
        .as_millis();
    let response = InspectionResponse {
        operation: "inspectDiagnostic",
        protocol: PROTOCOL,
        protocol_version: OBSERVATION_PROTOCOL_VERSION,
        installation_id,
        implementation: "celld",
        implementation_version: env!("CARGO_PKG_VERSION"),
        process_instance_id,
        cell_id: &request.cell_id,
        scope: &scope,
        local_presence,
        epoch,
        observed_at_ms,
        nonce: &request.nonce,
        proof: sign(
            secret,
            &inspection_response_material(
                installation_id,
                env!("CARGO_PKG_VERSION"),
                process_instance_id,
                &request.cell_id,
                &scope,
                local_presence,
                epoch,
                observed_at_ms,
                &request.nonce,
            ),
        ),
    };
    write_json(&mut stream, &response).await
}

async fn reject_busy(mut stream: UnixStream) -> anyhow::Result<()> {
    write_json(
        &mut stream,
        &Rejection {
            error: "capacity_exhausted",
        },
    )
    .await
}

async fn execute_diagnostic(app: &AppHandle, scope: String) -> anyhow::Result<(u16, String)> {
    let Routed { request, route } = app
        .request(scope.clone())
        .await
        .map_err(|error| anyhow::anyhow!("actor refused diagnostic route: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "diagnostic execution is not local");
    let local = app.local_request(request, scope.clone(), None, "base_control");
    let completed = local
        .run(async {
            let host = app
                .execution_host()
                .context("celld has no execution runtime")?;
            host.fetch_cell(
                scope,
                None,
                RuntimeFetch {
                    url: "http://base-control/diagnostic".to_owned(),
                    method: "GET".to_owned(),
                    body: RequestBody::Bytes(Bytes::new()),
                    headers: Vec::new(),
                    request_id: None,
                    order: None,
                    parent: None,
                },
                None,
            )
            .await
        })
        .await;
    let response = match completed.result {
        Ok(response) => response,
        Err(LocalRequestFailure::Handler(error)) => return Err(error),
        Err(LocalRequestFailure::OutputGate { verdict, handler }) => {
            return Err(
                handler.unwrap_or_else(|| anyhow::anyhow!("output gate refused: {verdict:?}"))
            )
        }
    };
    anyhow::ensure!(
        response.stream.is_none() && response.websocket.is_none(),
        "diagnostic response must be buffered HTTP"
    );
    anyhow::ensure!(
        response.body.len() <= MAX_DIAGNOSTIC_RESPONSE_BYTES,
        "diagnostic response exceeds its maximum size"
    );
    let body = String::from_utf8(response.body).context("diagnostic response is not UTF-8")?;
    Ok((response.status, body))
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

fn valid_cell_id(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
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

fn execution_request_material(version: u32, operation: &str, cell_id: &str, nonce: &str) -> String {
    format!("execution:{PROTOCOL}:{version}:{operation}:{cell_id}:{nonce}")
}

#[allow(
    clippy::too_many_arguments,
    reason = "the signed observation wire contract names every authenticated field"
)]
fn inspection_response_material(
    installation_id: &str,
    implementation_version: &str,
    process_instance_id: &str,
    cell_id: &str,
    scope: &str,
    local_presence: &str,
    epoch: Option<u64>,
    observed_at_ms: u128,
    nonce: &str,
) -> String {
    format!(
        "inspection-response:{PROTOCOL}:{OBSERVATION_PROTOCOL_VERSION}:{installation_id}:celld:{implementation_version}:{process_instance_id}:{cell_id}:{scope}:{local_presence}:{}:{observed_at_ms}:{nonce}",
        epoch.map_or_else(String::new, |value| value.to_string()),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the signed wire contract names every independently authenticated response field"
)]
fn execution_response_material(
    operation: &str,
    installation_id: &str,
    implementation_version: &str,
    cell_id: &str,
    scope: &str,
    outcome: &str,
    status: Option<u16>,
    body: Option<&str>,
    nonce: &str,
) -> String {
    let body = body.unwrap_or("");
    format!(
        "execution-response:{PROTOCOL}:{PROTOCOL_VERSION}:{operation}:{installation_id}:celld:{implementation_version}:{cell_id}:{scope}:{outcome}:{}:{}:{nonce}",
        status.map_or_else(String::new, |status| status.to_string()),
        hex::encode(sha2::Sha256::digest(body.as_bytes())),
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

    #[test]
    fn execution_proof_is_bound_to_the_fixed_operation_and_cell() {
        let secret = [7_u8; 32];
        let nonce = "0123456789abcdef0123456789abcdef";
        let cell = "01234567-89ab-cdef-0123-456789abcdef";
        let proof = sign(
            &secret,
            &execution_request_material(PROTOCOL_VERSION, "executeDiagnostic", cell, nonce),
        );
        assert!(verify(
            &secret,
            &execution_request_material(PROTOCOL_VERSION, "executeDiagnostic", cell, nonce),
            &proof
        ));
        assert!(!verify(
            &secret,
            &execution_request_material(PROTOCOL_VERSION, "evictDiagnostic", cell, nonce),
            &proof
        ));
        assert!(!verify(
            &secret,
            &execution_request_material(
                PROTOCOL_VERSION,
                "executeDiagnostic",
                "fedcba98-7654-3210-fedc-ba9876543210",
                nonce,
            ),
            &proof
        ));
    }

    #[test]
    fn replay_guard_rejects_an_identical_recent_mutation_only() {
        let guard = ReplayGuard::default();
        assert!(guard.record_if_new("executeDiagnostic:cell:nonce-a".to_owned()));
        assert!(!guard.record_if_new("executeDiagnostic:cell:nonce-a".to_owned()));
        assert!(guard.record_if_new("executeDiagnostic:cell:nonce-b".to_owned()));
        assert!(guard.record_if_new("evictDiagnostic:cell:nonce-a".to_owned()));
    }

    #[test]
    fn inspection_proof_binds_presence_epoch_and_process_instance() {
        let secret = [7_u8; 32];
        let installation = "01234567-89ab-cdef-0123-456789abcdef";
        let process = "fedcba98-7654-3210-fedc-ba9876543210";
        let material = inspection_response_material(
            installation,
            "0.6.2",
            process,
            installation,
            "BaseDiagnostic:01234567-89ab-cdef-0123-456789abcdef",
            "locally_present",
            Some(3),
            1_700_000_000_000,
            "0123456789abcdef0123456789abcdef",
        );
        let proof = sign(&secret, &material);
        assert!(verify(&secret, &material, &proof));
        assert!(!verify(
            &secret,
            &inspection_response_material(
                installation,
                "0.6.2",
                process,
                installation,
                "BaseDiagnostic:01234567-89ab-cdef-0123-456789abcdef",
                "locally_absent",
                None,
                1_700_000_000_000,
                "0123456789abcdef0123456789abcdef",
            ),
            &proof,
        ));
    }
}

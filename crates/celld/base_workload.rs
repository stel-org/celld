//! Base-owned service Binding callbacks for retained Base realizations.
//!
//! This is intentionally distinct from celld's deployment-local Service
//! graph. The identity here is minted by `base_control` from the retained
//! realization metadata and is never supplied by JavaScript.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context};
use base64::Engine as _;
use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::worker_realization::RealizationMetadata;

const PROTOCOL: &str = "base-workload-callback";
const PROTOCOL_VERSION: u32 = 1;
const OPERATION: &str = "dispatchServiceBinding";
const SECRET_PROTOCOL_VERSION: u32 = 2;
const SECRET_OPERATION: &str = "readSecretBinding";
const SECRET_RESPONSE_KEY_LABEL: &[u8] = b"base-workload-secret-response-key-v1";
const DATA_PROTOCOL_VERSION: u32 = 3;
const DATA_OPERATION: &str = "dataServiceOperation";
const DATA_RESPONSE_KEY_LABEL: &[u8] = b"base-workload-data-response-key-v1";
const MAX_METADATA_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_BYTES: usize = 96 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_DATA_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 2_048;
const MAX_CONTENT_TYPE_BYTES: usize = 256;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(8);

type HmacSha256 = Hmac<Sha256>;

/// Trusted, immutable facts installed only into Base-managed retained Worker
/// configurations. It contains no application-visible capability or getter.
#[derive(Clone)]
pub(crate) struct BaseWorkloadContext {
    socket_path: PathBuf,
    installation_id: String,
    process_instance_id: String,
    secret: Arc<[u8]>,
    metadata: RealizationMetadata,
}

impl BaseWorkloadContext {
    pub(crate) fn new(
        socket_path: PathBuf,
        installation_id: String,
        process_instance_id: String,
        secret: Arc<[u8]>,
        metadata: RealizationMetadata,
    ) -> Self {
        Self {
            socket_path,
            installation_id,
            process_instance_id,
            secret,
            metadata,
        }
    }

    /// Performs exactly one bounded Base Binding request. Arguments arrive
    /// from JS, but source identity always comes from `metadata` above.
    pub(crate) async fn dispatch(
        &self,
        binding_name: String,
        url: String,
        method: String,
        content_type: Option<String>,
        body: Vec<u8>,
    ) -> Result<BaseWorkloadResponse, String> {
        if !valid_binding_name(&binding_name)
            || !valid_method(&method)
            || body.len() > MAX_BODY_BYTES
            || content_type
                .as_ref()
                .is_some_and(|value| !valid_content_type(value))
        {
            return Err("Base Service request is invalid".to_owned());
        }
        let path_and_query = path_from_service_url(&url)?;
        let nonce = nonce().map_err(|_| "Base Service request could not be prepared".to_owned())?;
        let mut request = CallbackRequest {
            protocol: PROTOCOL,
            protocol_version: PROTOCOL_VERSION,
            operation: OPERATION,
            installation_id: &self.installation_id,
            process_instance_id: &self.process_instance_id,
            project_id: &self.metadata.project_id,
            worker_id: &self.metadata.worker_id,
            branch_id: &self.metadata.branch_id,
            realization_id: &self.metadata.realization_id,
            binding_name: &binding_name,
            method: &method,
            path_and_query: &path_and_query,
            content_type: content_type.as_deref(),
            body_bytes: body.len(),
            body_sha256: hex::encode(Sha256::digest(&body)),
            nonce: &nonce,
            proof: String::new(),
        };
        request.proof = sign(&self.secret, &callback_material(&request));
        let operation = async {
            let mut stream = UnixStream::connect(&self.socket_path)
                .await
                .context("connect Base workload callback")?;
            write_json(&mut stream, &request).await?;
            stream
                .write_all(&body)
                .await
                .context("write Base workload body")?;
            stream.flush().await.context("flush Base workload body")?;
            let response = read_line(&mut stream, MAX_RESPONSE_BYTES).await?;
            let response: CallbackResponse =
                serde_json::from_slice(&response).context("decode Base workload response")?;
            if response.protocol != PROTOCOL
                || response.protocol_version != PROTOCOL_VERSION
                || response.operation != OPERATION
            {
                bail!("Base workload callback returned an invalid response");
            }
            if response.outcome != "confirmed" {
                bail!("Base workload callback rejected the service request");
            }
            let status = response
                .status
                .ok_or_else(|| anyhow!("Base workload response lacks status"))?;
            let body = base64::engine::general_purpose::STANDARD
                .decode(
                    response
                        .body_base64
                        .ok_or_else(|| anyhow!("Base workload response lacks body"))?,
                )
                .context("decode Base workload response body")?;
            if body.len() > MAX_BODY_BYTES {
                bail!("Base workload response exceeds bound");
            }
            Ok(BaseWorkloadResponse { status, body })
        };
        crate::asyncrt::timeout(CALLBACK_TIMEOUT, operation)
            .await
            .map_err(|_| "Base Service request outcome is unknown".to_owned())?
            .map_err(|_| "Base Service request was rejected".to_owned())
    }

    /// Reads one Secret through a current Base Binding. JavaScript chooses
    /// only the binding name; all source facts come from this retained
    /// realization's trusted metadata. The response is separately sealed so
    /// plaintext never enters the generic dispatch response format.
    pub(crate) async fn read_secret(&self, binding_name: String) -> Result<Vec<u8>, String> {
        if !valid_binding_name(&binding_name) {
            return Err("Base Secret Binding request is invalid".to_owned());
        }
        let nonce =
            nonce().map_err(|_| "Base Secret Binding request could not be prepared".to_owned())?;
        let mut request = SecretCallbackRequest {
            protocol: PROTOCOL,
            protocol_version: SECRET_PROTOCOL_VERSION,
            operation: SECRET_OPERATION,
            installation_id: &self.installation_id,
            process_instance_id: &self.process_instance_id,
            project_id: &self.metadata.project_id,
            worker_id: &self.metadata.worker_id,
            branch_id: &self.metadata.branch_id,
            realization_id: &self.metadata.realization_id,
            binding_name: &binding_name,
            nonce: &nonce,
            proof: String::new(),
        };
        request.proof = sign(&self.secret, &secret_callback_material(&request));
        let operation = async {
            let mut stream = UnixStream::connect(&self.socket_path)
                .await
                .context("connect Base Secret callback")?;
            write_json(&mut stream, &request).await?;
            let response = read_line(&mut stream, MAX_RESPONSE_BYTES).await?;
            let response: SecretCallbackResponse =
                serde_json::from_slice(&response).context("decode Base Secret response")?;
            if response.protocol != PROTOCOL
                || response.protocol_version != SECRET_PROTOCOL_VERSION
                || response.operation != SECRET_OPERATION
            {
                bail!("Base Secret callback returned an invalid response");
            }
            if response.outcome != "confirmed" {
                bail!("Base Secret callback rejected the request");
            }
            let nonce = hex::decode(
                response
                    .nonce
                    .ok_or_else(|| anyhow!("Base Secret response lacks nonce"))?,
            )?;
            if nonce.len() != 24 {
                bail!("Base Secret response nonce is invalid");
            }
            let ciphertext = base64::engine::general_purpose::STANDARD
                .decode(
                    response
                        .ciphertext_base64
                        .ok_or_else(|| anyhow!("Base Secret response lacks ciphertext"))?,
                )
                .context("decode Base Secret ciphertext")?;
            // XChaCha20-Poly1305 has a 16-byte authentication tag. The
            // encrypted transport response is bounded before allocation by
            // the line limit and again here before decryption.
            if !(16..=MAX_BODY_BYTES + 16).contains(&ciphertext.len()) {
                bail!("Base Secret response exceeds bound");
            }
            let key = secret_response_key(&self.secret)?;
            let cipher = XChaCha20Poly1305::new_from_slice(&key)
                .map_err(|_| anyhow!("Base Secret transport key is invalid"))?;
            let plaintext = cipher
                .decrypt(
                    XNonce::from_slice(&nonce),
                    Payload {
                        msg: &ciphertext,
                        aad: secret_response_associated_data(&request).as_bytes(),
                    },
                )
                .map_err(|_| anyhow!("Base Secret response authentication failed"))?;
            if plaintext.len() > MAX_BODY_BYTES {
                bail!("Base Secret response exceeds bound");
            }
            Ok(plaintext)
        };
        crate::asyncrt::timeout(CALLBACK_TIMEOUT, operation)
            .await
            .map_err(|_| "Base Secret Binding request timed out".to_owned())?
            .map_err(|_| "Base Secret Binding request was rejected".to_owned())
    }

    /// Performs one typed Base data-Service operation.  JavaScript supplies a
    /// Binding name, a closed JSON operation and (only for Object upload) raw
    /// payload bytes. Source identity remains entirely derived from the
    /// retained realization metadata installed by Base control.
    pub(crate) async fn data_operation(
        &self,
        binding_name: String,
        data_operation: serde_json::Value,
        payload: Vec<u8>,
    ) -> Result<serde_json::Value, String> {
        if !valid_binding_name(&binding_name)
            || payload.len() > MAX_DATA_BODY_BYTES
            || serde_json::to_vec(&data_operation)
                .map_or(true, |bytes| bytes.len() > MAX_METADATA_BYTES / 2)
        {
            return Err("Base data Service request is invalid".to_owned());
        }
        let nonce =
            nonce().map_err(|_| "Base data Service request could not be prepared".to_owned())?;
        let mut request = DataCallbackRequest {
            protocol: PROTOCOL,
            protocol_version: DATA_PROTOCOL_VERSION,
            operation: DATA_OPERATION,
            installation_id: &self.installation_id,
            process_instance_id: &self.process_instance_id,
            project_id: &self.metadata.project_id,
            worker_id: &self.metadata.worker_id,
            branch_id: &self.metadata.branch_id,
            realization_id: &self.metadata.realization_id,
            binding_name: &binding_name,
            data_operation: &data_operation,
            payload_bytes: payload.len(),
            payload_sha256: hex::encode(Sha256::digest(&payload)),
            nonce: &nonce,
            proof: String::new(),
        };
        request.proof = sign(&self.secret, &data_callback_material(&request));
        let operation = async {
            let mut stream = UnixStream::connect(&self.socket_path)
                .await
                .context("connect Base data callback")?;
            write_json(&mut stream, &request).await?;
            stream
                .write_all(&payload)
                .await
                .context("write Base data payload")?;
            stream.flush().await.context("flush Base data payload")?;
            let response = read_line(&mut stream, MAX_RESPONSE_BYTES).await?;
            let response: DataCallbackResponse =
                serde_json::from_slice(&response).context("decode Base data response")?;
            if response.protocol != PROTOCOL
                || response.protocol_version != DATA_PROTOCOL_VERSION
                || response.operation != DATA_OPERATION
            {
                bail!("Base data callback returned an invalid response");
            }
            if response.outcome != "confirmed" && response.outcome != "stream" {
                bail!(
                    "Base data callback rejected the request (outcome={}): {}",
                    response.outcome,
                    response.error.as_deref().unwrap_or("rejected")
                );
            }
            let nonce = hex::decode(
                response
                    .nonce
                    .ok_or_else(|| anyhow!("Base data response lacks nonce"))?,
            )?;
            if nonce.len() != 24 {
                bail!("Base data response nonce is invalid");
            }
            let ciphertext = base64::engine::general_purpose::STANDARD.decode(
                response
                    .ciphertext_base64
                    .ok_or_else(|| anyhow!("Base data response lacks ciphertext"))?,
            )?;
            if !(16..=MAX_RESPONSE_BYTES).contains(&ciphertext.len()) {
                bail!("Base data response exceeds bound");
            }
            let key = data_response_key(&self.secret)?;
            let cipher = XChaCha20Poly1305::new_from_slice(&key)
                .map_err(|_| anyhow!("Base data transport key is invalid"))?;
            let plaintext = cipher
                .decrypt(
                    XNonce::from_slice(&nonce),
                    Payload {
                        msg: &ciphertext,
                        aad: data_response_associated_data(&request).as_bytes(),
                    },
                )
                .map_err(|_| anyhow!("Base data response authentication failed"))?;
            if response.outcome == "confirmed" {
                return serde_json::from_slice(&plaintext).context("decode Base data payload");
            }
            let descriptor: serde_json::Value =
                serde_json::from_slice(&plaintext).context("decode Base data stream descriptor")?;
            let expected = response
                .streamed_bytes
                .ok_or_else(|| anyhow!("Base data stream lacks size"))?;
            if expected > MAX_DATA_BODY_BYTES {
                bail!("Base data stream exceeds bound");
            }
            let mut result = Vec::with_capacity(expected);
            let mut sequence = 0u64;
            loop {
                let length = stream.read_u32().await? as usize;
                if length == 0 {
                    break;
                }
                if length > 256 * 1024 || result.len().saturating_add(length) > expected {
                    bail!("Base data stream frame exceeds bound");
                }
                let mut proof = [0_u8; 64];
                stream.read_exact(&mut proof).await?;
                let proof =
                    std::str::from_utf8(&proof).context("Base data stream proof is not text")?;
                let mut chunk = vec![0_u8; length];
                stream.read_exact(&mut chunk).await?;
                if !verify(
                    &self.secret,
                    &data_stream_chunk_material(&request, sequence, &chunk),
                    proof,
                ) {
                    bail!("Base data stream authentication failed");
                }
                result.extend_from_slice(&chunk);
                sequence = sequence.saturating_add(1);
            }
            if result.len() != expected {
                bail!("Base data stream ended early");
            }
            Ok(serde_json::json!({
                "bytes": base64::engine::general_purpose::STANDARD.encode(result),
                "descriptor": descriptor,
            }))
        };
        crate::asyncrt::timeout(CALLBACK_TIMEOUT, operation)
            .await
            .map_err(|_| "Base data Service request outcome is unknown".to_owned())?
            .map_err(|error| format!("Base data Service request was rejected: {error}"))
    }
}

pub(crate) struct BaseWorkloadResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CallbackRequest<'a> {
    protocol: &'static str,
    protocol_version: u32,
    operation: &'static str,
    installation_id: &'a str,
    process_instance_id: &'a str,
    project_id: &'a str,
    worker_id: &'a str,
    branch_id: &'a str,
    realization_id: &'a str,
    binding_name: &'a str,
    method: &'a str,
    path_and_query: &'a str,
    content_type: Option<&'a str>,
    body_bytes: usize,
    body_sha256: String,
    nonce: &'a str,
    proof: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CallbackResponse {
    protocol: String,
    protocol_version: u32,
    operation: String,
    outcome: String,
    status: Option<u16>,
    body_base64: Option<String>,
    #[allow(dead_code)]
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SecretCallbackRequest<'a> {
    protocol: &'static str,
    protocol_version: u32,
    operation: &'static str,
    installation_id: &'a str,
    process_instance_id: &'a str,
    project_id: &'a str,
    worker_id: &'a str,
    branch_id: &'a str,
    realization_id: &'a str,
    binding_name: &'a str,
    nonce: &'a str,
    proof: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SecretCallbackResponse {
    protocol: String,
    protocol_version: u32,
    operation: String,
    outcome: String,
    nonce: Option<String>,
    ciphertext_base64: Option<String>,
    #[allow(dead_code)]
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataCallbackRequest<'a> {
    protocol: &'static str,
    protocol_version: u32,
    operation: &'static str,
    installation_id: &'a str,
    process_instance_id: &'a str,
    project_id: &'a str,
    worker_id: &'a str,
    branch_id: &'a str,
    realization_id: &'a str,
    binding_name: &'a str,
    data_operation: &'a serde_json::Value,
    payload_bytes: usize,
    payload_sha256: String,
    nonce: &'a str,
    proof: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DataCallbackResponse {
    protocol: String,
    protocol_version: u32,
    operation: String,
    outcome: String,
    nonce: Option<String>,
    ciphertext_base64: Option<String>,
    #[serde(default)]
    streamed_bytes: Option<usize>,
    #[allow(dead_code)]
    error: Option<String>,
}

fn callback_material(request: &CallbackRequest<'_>) -> String {
    canonical_material(
        "celld-to-base-workload-callback-v1",
        &[
            request.protocol,
            &request.protocol_version.to_string(),
            request.operation,
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            request.method,
            request.path_and_query,
            request.content_type.unwrap_or(""),
            &request.body_bytes.to_string(),
            &request.body_sha256,
            request.nonce,
        ],
    )
}

fn secret_callback_material(request: &SecretCallbackRequest<'_>) -> String {
    canonical_material(
        "celld-to-base-secret-consumption-v1",
        &[
            request.protocol,
            &request.protocol_version.to_string(),
            request.operation,
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            request.nonce,
        ],
    )
}

fn secret_response_associated_data(request: &SecretCallbackRequest<'_>) -> String {
    canonical_material(
        "base-to-celld-secret-response-aad-v1",
        &[
            request.protocol,
            &request.protocol_version.to_string(),
            request.operation,
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            request.nonce,
        ],
    )
}

fn data_operation_material(operation: &serde_json::Value) -> String {
    serde_json::to_string(operation).unwrap_or_default()
}

fn data_callback_material(request: &DataCallbackRequest<'_>) -> String {
    let operation = data_operation_material(request.data_operation);
    canonical_material(
        "celld-to-base-data-service-callback-v1",
        &[
            request.protocol,
            &request.protocol_version.to_string(),
            request.operation,
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            &operation,
            &request.payload_bytes.to_string(),
            &request.payload_sha256,
            request.nonce,
        ],
    )
}

fn data_response_associated_data(request: &DataCallbackRequest<'_>) -> String {
    let operation = data_operation_material(request.data_operation);
    canonical_material(
        "base-to-celld-data-service-response-aad-v1",
        &[
            request.protocol,
            &request.protocol_version.to_string(),
            request.operation,
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            &operation,
            &request.payload_bytes.to_string(),
            &request.payload_sha256,
            request.nonce,
        ],
    )
}

fn data_stream_chunk_material(
    request: &DataCallbackRequest<'_>,
    sequence: u64,
    chunk: &[u8],
) -> String {
    canonical_material(
        "base-to-celld-data-service-stream-chunk-v1",
        &[
            request.installation_id,
            request.process_instance_id,
            request.project_id,
            request.worker_id,
            request.branch_id,
            request.realization_id,
            request.binding_name,
            request.nonce,
            &sequence.to_string(),
            &chunk.len().to_string(),
            &hex::encode(Sha256::digest(chunk)),
        ],
    )
}

fn canonical_material(label: &str, fields: &[&str]) -> String {
    let mut material = String::from(label);
    for value in fields {
        material.push(':');
        material.push_str(&value.len().to_string());
        material.push(':');
        material.push_str(value);
    }
    material
}

fn sign(secret: &[u8], material: &str) -> String {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(secret).expect("Base pairing secret is valid");
    mac.update(material.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify(secret: &[u8], material: &str, proof: &str) -> bool {
    let Ok(tag) = hex::decode(proof) else {
        return false;
    };
    let Ok(mut mac) = <HmacSha256 as Mac>::new_from_slice(secret) else {
        return false;
    };
    mac.update(material.as_bytes());
    mac.verify_slice(&tag).is_ok()
}

fn secret_response_key(secret: &[u8]) -> anyhow::Result<[u8; 32]> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)?;
    mac.update(SECRET_RESPONSE_KEY_LABEL);
    let bytes = mac.finalize().into_bytes();
    let mut key = [0_u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn data_response_key(secret: &[u8]) -> anyhow::Result<[u8; 32]> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)?;
    mac.update(DATA_RESPONSE_KEY_LABEL);
    let bytes = mac.finalize().into_bytes();
    let mut key = [0_u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn path_from_service_url(value: &str) -> Result<String, String> {
    let url = url::Url::parse(value).map_err(|_| "Base Service URL is invalid".to_owned())?;
    if url.scheme() != "https"
        || url.host_str() != Some("service.internal")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("Base Service URL is invalid".to_owned());
    }
    let mut path = url.path().to_owned();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err("Base Service URL is invalid".to_owned());
    }
    Ok(path)
}

fn valid_method(value: &str) -> bool {
    matches!(value, "GET" | "POST" | "PUT" | "PATCH" | "DELETE")
}
fn valid_binding_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}
fn valid_content_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTENT_TYPE_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_control())
}
fn nonce() -> anyhow::Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    Ok(hex::encode(bytes))
}

async fn write_json(stream: &mut UnixStream, value: &impl Serialize) -> anyhow::Result<()> {
    let mut body = serde_json::to_vec(value)?;
    if body.len() > MAX_METADATA_BYTES {
        bail!("Base workload callback metadata exceeds bound");
    }
    body.push(b'\n');
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}
async fn read_line(stream: &mut UnixStream, limit: usize) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::with_capacity(256);
    loop {
        let mut byte = [0_u8; 1];
        if stream.read(&mut byte).await? == 0 {
            bail!("Base workload callback closed early");
        }
        if byte[0] == b'\n' {
            return Ok(body);
        }
        if body.len() >= limit {
            bail!("Base workload callback response exceeds bound");
        }
        body.push(byte[0]);
    }
}

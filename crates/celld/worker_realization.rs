//! Bounded, process-local ownership of Base Worker realizations.
//!
//! This is intentionally an execution-infrastructure component rather than a
//! Base control transport concern.  A realization retains a `StatelessRuntime`
//! (and therefore its existing isolate-pool infrastructure), but it is not a
//! promise that a particular V8 isolate is continuously resident or executing.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::bail;
use tokio::sync::Notify;

use crate::{
    engine_api::{HttpResponse, RequestBody},
    runtime::StatelessRuntime,
};

/// A conservative process-wide ceiling.  This bounds retained pools and
/// artifacts independently of the control socket's per-connection admission.
pub const MAX_REALIZATIONS: usize = 16;
const RETIRE_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealizationMetadata {
    pub realization_id: String,
    pub project_id: String,
    pub worker_id: String,
    pub branch_id: String,
    pub cell_id: String,
    pub artifact_sha256: String,
    pub runtime_profile: String,
    pub execution_contract: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalPresence {
    Accepting,
    Retiring,
    Absent,
}

#[derive(Debug)]
pub enum EstablishError {
    AlreadyExists,
    CapacityExhausted,
    Runtime(anyhow::Error),
}

#[derive(Debug)]
pub enum InvokeError {
    Absent,
    Retiring,
    Runtime(anyhow::Error),
}

#[derive(Debug)]
pub enum RetireResult {
    Retired,
    Retiring,
    Absent,
}

struct Entry {
    metadata: RealizationMetadata,
    runtime: StatelessRuntime,
    state: Mutex<EntryState>,
    drained: Notify,
}

struct EntryState {
    accepting: bool,
    in_flight: usize,
}

/// The sole owner of retained Base Worker runtimes for one celld process.
#[derive(Default)]
pub struct WorkerRealizationRegistry {
    entries: Mutex<HashMap<String, Arc<Entry>>>,
}

impl WorkerRealizationRegistry {
    pub fn establish(
        &self,
        metadata: RealizationMetadata,
        runtime: StatelessRuntime,
    ) -> Result<(), EstablishError> {
        let mut entries = self.entries.lock().map_err(|_| {
            EstablishError::Runtime(anyhow::anyhow!("Worker realization registry lock poisoned"))
        })?;
        if entries.contains_key(&metadata.realization_id) {
            return Err(EstablishError::AlreadyExists);
        }
        if entries.len() >= MAX_REALIZATIONS {
            return Err(EstablishError::CapacityExhausted);
        }
        entries.insert(
            metadata.realization_id.clone(),
            Arc::new(Entry {
                metadata,
                runtime,
                state: Mutex::new(EntryState {
                    accepting: true,
                    in_flight: 0,
                }),
                drained: Notify::new(),
            }),
        );
        Ok(())
    }

    pub fn inspect(&self, realization_id: &str) -> Option<(RealizationMetadata, LocalPresence)> {
        let entry = self.entry(realization_id)?;
        let state = entry.state.lock().ok()?;
        Some((
            entry.metadata.clone(),
            if state.accepting {
                LocalPresence::Accepting
            } else {
                LocalPresence::Retiring
            },
        ))
    }

    pub async fn invoke(
        self: &Arc<Self>,
        metadata: &RealizationMetadata,
        url: String,
        method: String,
    ) -> Result<HttpResponse, InvokeError> {
        let Some(entry) = self.entry(&metadata.realization_id) else {
            return Err(InvokeError::Absent);
        };
        if entry.metadata != *metadata {
            return Err(InvokeError::Absent);
        }
        {
            let mut state = entry.state.lock().map_err(|_| {
                InvokeError::Runtime(anyhow::anyhow!("Worker realization state lock poisoned"))
            })?;
            if !state.accepting {
                return Err(InvokeError::Retiring);
            }
            state.in_flight += 1;
        }
        let result = entry
            .runtime
            .fetch(url, method, RequestBody::from(Vec::new()), Vec::new(), None)
            .await
            .map_err(InvokeError::Runtime);
        self.release(&metadata.realization_id, &entry);
        result
    }

    pub async fn retire(self: &Arc<Self>, metadata: &RealizationMetadata) -> RetireResult {
        let Some(entry) = self.entry(&metadata.realization_id) else {
            return RetireResult::Absent;
        };
        if entry.metadata != *metadata {
            return RetireResult::Absent;
        }
        {
            let Ok(mut state) = entry.state.lock() else {
                return RetireResult::Retiring;
            };
            state.accepting = false;
            if state.in_flight == 0 {
                drop(state);
                self.remove_exact(&metadata.realization_id, &entry);
                return RetireResult::Retired;
            }
        }
        let drained = crate::asyncrt::timeout(RETIRE_DRAIN_TIMEOUT, async {
            loop {
                entry.drained.notified().await;
                if entry.state.lock().is_ok_and(|state| state.in_flight == 0) {
                    return;
                }
            }
        })
        .await;
        if drained.is_ok() {
            self.remove_exact(&metadata.realization_id, &entry);
            RetireResult::Retired
        } else {
            RetireResult::Retiring
        }
    }

    fn entry(&self, realization_id: &str) -> Option<Arc<Entry>> {
        self.entries.lock().ok()?.get(realization_id).cloned()
    }

    fn release(&self, realization_id: &str, entry: &Arc<Entry>) {
        let remove = entry.state.lock().is_ok_and(|mut state| {
            state.in_flight = state.in_flight.saturating_sub(1);
            if state.in_flight == 0 {
                entry.drained.notify_waiters();
                !state.accepting
            } else {
                false
            }
        });
        if remove {
            self.remove_exact(realization_id, entry);
        }
    }

    fn remove_exact(&self, realization_id: &str, expected: &Arc<Entry>) {
        if let Ok(mut entries) = self.entries.lock() {
            if entries
                .get(realization_id)
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                entries.remove(realization_id);
            }
        }
    }
}

impl From<EstablishError> for anyhow::Error {
    fn from(value: EstablishError) -> Self {
        match value {
            EstablishError::AlreadyExists => anyhow::anyhow!("Worker realization already exists"),
            EstablishError::CapacityExhausted => {
                anyhow::anyhow!("Worker realization capacity exhausted")
            }
            EstablishError::Runtime(error) => error,
        }
    }
}

/// Ensures a response remains suitable for the bounded Base control protocol.
pub fn bounded_response(response: HttpResponse, maximum: usize) -> anyhow::Result<HttpResponse> {
    if response.stream.is_some() || response.websocket.is_some() || response.body.len() > maximum {
        bail!("Worker realization response is not a bounded HTTP body");
    }
    Ok(response)
}

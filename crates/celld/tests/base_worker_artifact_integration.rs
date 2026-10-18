//! Reusable Base Worker artifact preparation and celld execution coverage.

use celld::base_worker_artifact::{
    BaseWorkerArtifact, BaseWorkerArtifactError, BaseWorkerArtifactPreparer,
};
use celld::engine_api::{Compat, WorkerConfigOptions};
use celld::js::{RequestBody, WorkerConfig};
use celld::runtime::StatelessRuntime;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};

fn init_v8() {
    static INIT: Once = Once::new();
    INIT.call_once(celld::runtime::init_v8);
}

fn init_async_runtime() {
    static INIT: Once = Once::new();
    INIT.call_once(|| celld::asyncrt::set_host_handle(tokio::runtime::Handle::current()));
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/base-worker-reusable")
        .join(name)
}

fn worker_binary() -> PathBuf {
    std::env::var_os("BASE_WORKER_BIN")
        .map(PathBuf::from)
        .expect("BASE_WORKER_BIN must select the Base Worker fork executable")
}

fn options(artifact: BaseWorkerArtifact) -> WorkerConfigOptions {
    WorkerConfigOptions {
        src: artifact.into_source(),
        script_name: "base-worker-artifact-integration".to_string(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        r2_bindings: Vec::new(),
        d1_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: "base-worker-artifact-node".to_string(),
        modules: Vec::new(),
        compat: Compat::default(),
    }
}

async fn execute(artifact: BaseWorkerArtifact, path: &str) -> String {
    let runtime = StatelessRuntime::start(
        Arc::new(WorkerConfig::new(options(artifact))),
        Arc::from("base-worker-artifact-node"),
        Arc::from("base-worker-artifact-region"),
    )
    .expect("celld loads the Base Worker artifact through its existing host");
    let response = runtime
        .fetch(
            format!("https://base.test{path}"),
            "GET".to_string(),
            RequestBody::from(Vec::new()),
            Vec::new(),
            None,
        )
        .await
        .expect("celld dispatches the prepared artifact");
    assert_eq!(response.status, 200);
    String::from_utf8(response.body).expect("response JSON is UTF-8")
}

#[test]
fn missing_base_worker_executable_is_explicit_and_never_uses_path_lookup() {
    let error = BaseWorkerArtifactPreparer::new("/definitely/not/a/base-worker-deno")
        .expect_err("a missing explicit executable is rejected");
    assert!(matches!(
        error,
        BaseWorkerArtifactError::ExecutableUnavailable { .. }
    ));
    assert!(error
        .to_string()
        .contains("Base Worker executable unavailable"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires BASE_WORKER_BIN; run scripts/prove-base-worker-artifacts.sh"]
async fn distinct_base_worker_inputs_prepare_and_execute_independently() {
    init_v8();
    init_async_runtime();
    let preparer = BaseWorkerArtifactPreparer::new(worker_binary())
        .expect("the selected Base Worker fork executable is available");

    let typescript = preparer
        .prepare(fixture("typescript-worker.ts"))
        .expect("TypeScript preparation succeeds");
    assert_eq!(typescript.entrypoint(), fixture("typescript-worker.ts"));
    assert!(typescript.source().contains("typescript"));
    assert!(!typescript.source().contains("interface PreparedMessage"));
    assert!(typescript.byte_len() > 0);
    assert_eq!(typescript.sha256().len(), 64);

    let javascript = preparer
        .prepare(fixture("javascript-worker.js"))
        .expect("JavaScript preparation succeeds");
    assert_eq!(javascript.entrypoint(), fixture("javascript-worker.js"));
    assert!(javascript.source().contains("javascript"));
    assert_ne!(typescript.sha256(), javascript.sha256());

    assert_eq!(
        execute(typescript, "/typescript").await,
        r#"{"worker":"typescript","path":"/typescript"}"#
    );
    assert_eq!(
        execute(javascript, "/javascript").await,
        r#"{"worker":"javascript","path":"/javascript"}"#
    );
}

#[test]
#[ignore = "requires BASE_WORKER_BIN; run scripts/prove-base-worker-artifacts.sh"]
fn invalid_source_and_missing_import_are_preparation_failures_without_artifact_reuse() {
    let preparer = BaseWorkerArtifactPreparer::new(worker_binary())
        .expect("the selected Base Worker fork executable is available");
    let previous = preparer
        .prepare(fixture("typescript-worker.ts"))
        .expect("a preceding valid preparation succeeds");

    for source in ["invalid-typescript.ts", "missing-local-import.ts"] {
        let error = preparer
            .prepare(fixture(source))
            .expect_err("invalid source preparation fails instead of returning a prior artifact");
        assert!(matches!(
            error,
            BaseWorkerArtifactError::PreparationFailed { .. }
        ));
        assert!(error.to_string().contains("Base Worker preparation failed"));
    }

    assert_eq!(previous.entrypoint(), fixture("typescript-worker.ts"));
    assert!(previous.source().contains("typescript"));
}

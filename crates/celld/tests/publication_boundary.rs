// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! New-publication policy regression coverage.

use celld::control_plane::{apply_deployment, AgentDeployment};
use celld::deploy::{self, Built, Options, VarOverrides};
use celld::protocol::{
    validate_native_publication, validate_required_features, DeployPointer, Manifest, Rollout,
    FEATURE_D1_V1, FEATURE_KV_V1, FEATURE_QUEUES_V1, FEATURE_R2_V1, FEATURE_WORKFLOWS_V1,
};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

fn manifest() -> Manifest {
    Manifest {
        schema_version: 1,
        version: "version".to_string(),
        script_name: "base-test".to_string(),
        main_module: Some("index.js".to_string()),
        do_classes: Vec::new(),
        sqlite_classes: Vec::new(),
        modules: Vec::new(),
        assets: None,
        crons: Vec::new(),
        queue_consumers: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        required_features: Vec::new(),
        raw_metadata: json!({ "bindings": [] }),
    }
}

fn options(config: &Path) -> Options {
    Options {
        config: Some(config.to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: true,
        json: false,
        vars: VarOverrides::None,
        local_images: false,
    }
}

#[allow(clippy::disallowed_methods)] // Test-owned source fixture, not celld node storage.
fn write_project(root: &Path, extra: Value) -> std::path::PathBuf {
    std::fs::write(
        root.join("index.js"),
        "export default { fetch() { return new Response('Base'); } };",
    )
    .expect("write worker");
    let mut config = json!({
        "name": "base-test",
        "main": "index.js",
        "no_bundle": true,
    });
    config
        .as_object_mut()
        .expect("config object")
        .extend(extra.as_object().expect("extra config object").clone());
    let path = root.join("wrangler.json");
    std::fs::write(&path, serde_json::to_vec(&config).expect("encode config"))
        .expect("write config");
    path
}

#[test]
fn local_product_declarations_are_rejected_before_building() {
    for (key, value) in [
        ("d1_databases", json!([])),
        ("kv_namespaces", json!([])),
        ("r2_buckets", json!([])),
        ("queues", json!({})),
        ("workflows", Value::Null),
    ] {
        let root = tempfile::tempdir().expect("temporary project");
        let config = write_project(root.path(), json!({ key: value }));
        let error = match deploy::build(&options(&config)) {
            Ok(_) => panic!("product declaration unexpectedly built"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(key),
            "{key} was not identified: {error:#}"
        );
    }
}

#[test]
fn local_product_declarations_report_all_present_keys_deterministically() {
    let root = tempfile::tempdir().expect("temporary project");
    let config = write_project(
        root.path(),
        json!({ "kv_namespaces": [], "d1_databases": [], "workflows": null }),
    );
    let error = match deploy::build(&options(&config)) {
        Ok(_) => panic!("product declarations unexpectedly built"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "configuration declares unsupported Base-native product keys: d1_databases, kv_namespaces, workflows"
    );
}

#[test]
fn ordinary_local_worker_build_remains_allowed() {
    let root = tempfile::tempdir().expect("temporary project");
    let config = write_project(root.path(), json!({}));
    let built = deploy::build(&options(&config)).expect("ordinary worker builds");
    validate_native_publication(&built.manifest).expect("ordinary manifest is publishable");
}

#[test]
fn legacy_features_remain_recognized_for_existing_manifest_loading() {
    validate_required_features(&[
        FEATURE_D1_V1.to_string(),
        FEATURE_KV_V1.to_string(),
        FEATURE_R2_V1.to_string(),
        FEATURE_QUEUES_V1.to_string(),
        FEATURE_WORKFLOWS_V1.to_string(),
    ])
    .expect("legacy feature identifiers remain reader-compatible");
}

#[test]
fn publication_validator_rejects_features_bindings_classes_and_metadata() {
    for feature in [
        FEATURE_D1_V1,
        FEATURE_KV_V1,
        FEATURE_R2_V1,
        FEATURE_QUEUES_V1,
        FEATURE_WORKFLOWS_V1,
    ] {
        let mut candidate = manifest();
        candidate.required_features.push(feature.to_string());
        let error = validate_native_publication(&candidate).expect_err("feature rejected");
        assert!(error.to_string().contains(feature));
    }
    for binding_type in ["d1", "kv", "r2_bucket", "queue", "workflow"] {
        let mut candidate = manifest();
        candidate.raw_metadata = json!({ "bindings": [{ "type": binding_type }] });
        let error = validate_native_publication(&candidate).expect_err("binding rejected");
        assert!(error.to_string().contains(binding_type));
    }
    for class in [
        celld::deploy::D1_CLASS,
        celld::deploy::KV_CLASS,
        celld::deploy::QUEUE_CLASS,
        celld::deploy::WORKFLOW_CLASS,
        "__Workflow.base-test",
    ] {
        let mut candidate = manifest();
        candidate.do_classes.push(class.to_string());
        let error = validate_native_publication(&candidate).expect_err("class rejected");
        assert!(error.to_string().contains(class));
    }
    for key in [
        "d1_databases",
        "kv_namespaces",
        "r2_buckets",
        "queues",
        "queue_consumers",
        "workflows",
    ] {
        let mut candidate = manifest();
        candidate.raw_metadata[key] = Value::Null;
        let error = validate_native_publication(&candidate).expect_err("metadata rejected");
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn publication_validator_rejects_malformed_metadata_and_queue_consumers() {
    let mut malformed = manifest();
    malformed.raw_metadata = json!({ "bindings": {} });
    assert!(validate_native_publication(&malformed)
        .expect_err("malformed bindings rejected")
        .to_string()
        .contains("bindings"));

    let mut queue = manifest();
    queue
        .queue_consumers
        .push(celld::protocol::QueueConsumerConfig {
            queue: "jobs".to_string(),
            max_batch_size: 1,
            max_batch_timeout: 1,
            max_retries: 1,
            dead_letter_queue: None,
            max_concurrency: None,
            retry_delay: None,
        });
    assert!(validate_native_publication(&queue)
        .expect_err("queue consumers rejected")
        .to_string()
        .contains("Queue consumers"));
}

#[test]
fn ordinary_manifest_shapes_remain_publishable() {
    for metadata in [
        json!({ "bindings": [{ "type": "plain_text", "name": "VALUE", "text": "x" }] }),
        json!({ "bindings": [{ "type": "service", "name": "API", "service": "api" }] }),
        json!({ "bindings": [{ "type": "worker_loader", "name": "LOADER" }] }),
        json!({ "bindings": [{ "type": "assets", "name": "ASSETS" }] }),
    ] {
        let mut candidate = manifest();
        candidate.raw_metadata = metadata;
        validate_native_publication(&candidate).expect("ordinary binding remains publishable");
    }
    let mut durable_object = manifest();
    durable_object.do_classes.push("Counter".to_string());
    durable_object.sqlite_classes.push("Counter".to_string());
    validate_native_publication(&durable_object)
        .expect("ordinary Durable Object remains publishable");
}

fn built(manifest: Manifest) -> Built {
    Built {
        script_name: manifest.script_name.clone(),
        version: manifest.version.clone(),
        prefix: format!("deploy/{}/{}", manifest.script_name, manifest.version),
        manifest,
        modules: Vec::new(),
        assets: None,
        bundled_in: Duration::ZERO,
        images: Vec::new(),
        fence_image: None,
        upload_images: false,
    }
}

fn managed_deployment(manifest: Manifest) -> AgentDeployment {
    AgentDeployment {
        script_name: manifest.script_name.clone(),
        version: manifest.version.clone(),
        pointer: DeployPointer {
            script_name: Some(manifest.script_name.clone()),
            version: manifest.version.clone(),
            prefix: format!("deploy/{}/{}", manifest.script_name, manifest.version),
            rollout: Rollout { percent: 100 },
        },
        manifest,
        modules: Vec::new(),
        assets: None,
    }
}

#[tokio::test]
async fn rejected_local_and_managed_publications_have_no_bucket_side_effects() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let root = tempfile::tempdir().expect("temporary bucket");
    let bucket = celld::dev::open_local_bucket(&root.path().join("bucket.sqlite"))
        .expect("open local bucket");

    let mut local_manifest = manifest();
    local_manifest
        .required_features
        .push(FEATURE_KV_V1.to_string());
    let error = deploy::write(&bucket, &built(local_manifest))
        .await
        .expect_err("local publication rejected");
    assert!(error.to_string().contains(FEATURE_KV_V1));
    assert!(bucket
        .get("deploy/current.json")
        .await
        .expect("read pointer")
        .is_none());
    assert!(bucket
        .get("deploy/base-test/current.json")
        .await
        .expect("read named pointer")
        .is_none());

    let mut managed_manifest = manifest();
    managed_manifest
        .required_features
        .push(FEATURE_R2_V1.to_string());
    let deployment = managed_deployment(managed_manifest);
    let client = reqwest::Client::new();
    let error = apply_deployment(&client, "unused", &bucket, &deployment)
        .await
        .expect_err("managed publication rejected");
    assert!(error.to_string().contains(FEATURE_R2_V1));
    assert!(bucket
        .get("deploy/current.json")
        .await
        .expect("read pointer")
        .is_none());
    assert!(bucket
        .get("deploy/base-test/version/manifest.json")
        .await
        .expect("read manifest")
        .is_none());
}

use celld::engine_api::{Compat, QueueBinding, WorkerConfigOptions, WorkflowBinding};
use celld::js::{RequestBody, Worker, WorkerConfig};
use celld::runtime::StatelessRuntime;
use std::sync::{Arc, Once};

fn init_v8() {
    static INIT: Once = Once::new();
    INIT.call_once(celld::runtime::init_v8);
}

fn init_async_runtime() {
    static INIT: Once = Once::new();
    INIT.call_once(|| celld::asyncrt::set_host_handle(tokio::runtime::Handle::current()));
}

fn options(src: &str) -> WorkerConfigOptions {
    WorkerConfigOptions {
        src: src.to_string(),
        script_name: "host-boundary".to_string(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        r2_bindings: Vec::new(),
        d1_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: "test-node".to_string(),
        modules: Vec::new(),
        compat: Compat::default(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn base_native_worker_executes_through_the_core_v8_host_without_products() {
    init_v8();
    init_async_runtime();
    let worker = WorkerConfig::new(options(
        r#"
        export default {
          async fetch(request, env) {
            const path = new URL(request.url).pathname;
            if (path === "/dynamic-workflow") {
              await import("cloudflare:workflows");
            }
            return Response.json({
              message: "Hello from Base",
              path,
              products: ["KV", "DB", "BUCKET", "QUEUE", "WORKFLOW"].filter((name) => name in env),
            });
          },
        };
        "#,
    ));

    // This exercises the real isolate, prelude, harness, module evaluation,
    // default-export discovery, stateless dispatch and response path without
    // any inherited product configuration.
    let runtime = StatelessRuntime::start(
        Arc::new(worker),
        Arc::from("test-node"),
        Arc::from("test-region"),
    )
    .expect("a plain Worker loads without product support");
    let response = runtime
        .fetch(
            "https://base.test/hello".to_string(),
            "GET".to_string(),
            RequestBody::from(Vec::new()),
            Vec::new(),
            None,
        )
        .await
        .expect("the Worker fetch succeeds");
    assert_eq!(response.status, 200);
    assert_eq!(
        String::from_utf8(response.body).expect("JSON response is UTF-8"),
        r#"{"message":"Hello from Base","path":"/hello","products":[]}"#
    );
    let result = runtime
        .fetch(
            "https://base.test/dynamic-workflow".to_string(),
            "GET".to_string(),
            RequestBody::from(Vec::new()),
            Vec::new(),
            None,
        )
        .await;
    let error = match result {
        Ok(_) => panic!("a dynamic Workflow import is rejected in the native host"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("cloudflare:workflows is unavailable for Base-native execution"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_workers_do_not_inherit_legacy_product_bindings() {
    init_v8();
    init_async_runtime();
    let mut parent_options = options(
        r#"
        export default {
          async fetch(request, env) {
            const child = env.LOADER.load({
              compatibilityDate: "2026-01-01",
              mainModule: "child.js",
              modules: {
                "child.js": { js: `
                  export default {
                    fetch(_request, childEnv) {
                      return Response.json({ hasKv: "KV" in childEnv });
                    },
                  };
                ` },
              },
            });
            return await child.getEntrypoint().fetch(request);
          },
        };
        "#,
    );
    parent_options
        .kv_bindings
        .push(("KV".to_string(), "legacy-parent-kv".to_string()));
    let parent = WorkerConfig::new(parent_options).with_loaders(vec!["LOADER".to_string()]);
    let runtime = StatelessRuntime::start(
        Arc::new(parent),
        Arc::from("test-node"),
        Arc::from("test-region"),
    )
    .expect("the parent Worker loads");

    let response = runtime
        .fetch(
            "https://base.test/loader".to_string(),
            "GET".to_string(),
            RequestBody::from(Vec::new()),
            Vec::new(),
            None,
        )
        .await
        .expect("the dynamic Worker fetch succeeds");
    assert_eq!(response.status, 200);
    assert_eq!(
        String::from_utf8(response.body).expect("JSON response is UTF-8"),
        r#"{"hasKv":false}"#
    );
}

#[test]
fn base_native_workflow_import_is_rejected_during_real_module_loading() {
    init_v8();
    let worker = WorkerConfig::new(options(
        r#"
        import { NonRetryableError } from "cloudflare:workflows";
        export default { fetch() { return new Response(NonRetryableError.name); } };
        "#,
    ));

    let error = match Worker::load_config(Arc::new(worker)) {
        Ok(_) => panic!("workflow imports are legacy-only"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("cloudflare:workflows is unavailable for Base-native execution"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn legacy_workflow_configuration_retains_its_runtime_registration() {
    init_v8();
    let mut config = options(
        r#"
        import { WorkflowEntrypoint } from "cloudflare:workers";
        import { NonRetryableError } from "cloudflare:workflows";
        export class LegacyFlow extends WorkflowEntrypoint {}
        export default { fetch() { return new Response(NonRetryableError.name); } };
        "#,
    );
    config.script_name = "legacy-workflow".to_string();
    config.do_classes = vec![celld::deploy::workflow_class("legacy-workflow")];
    config.workflow_bindings = vec![WorkflowBinding {
        environment: "FLOW".to_string(),
        workflow: "legacy-flow".to_string(),
        class: "LegacyFlow".to_string(),
    }];

    Worker::load_config(Arc::new(WorkerConfig::new(config)))
        .expect("a stored legacy Workflow deployment still receives its product registration");
}

#[test]
fn legacy_product_bindings_select_only_their_existing_host_registration() {
    init_v8();
    let source = "export default { fetch() { return new Response('legacy'); } };";

    let mut kv = options(source);
    kv.kv_bindings
        .push(("KV".to_string(), "legacy-kv".to_string()));

    let mut d1 = options(source);
    d1.d1_bindings
        .push(("DB".to_string(), "legacy-db".to_string()));

    let mut r2 = options(source);
    r2.r2_bindings
        .push(("BUCKET".to_string(), "legacy-bucket".to_string()));

    let mut queue = options(source);
    queue.queue_bindings.push(QueueBinding {
        environment: "QUEUE".to_string(),
        queue: "legacy-queue".to_string(),
        delivery_delay: 0,
    });

    for config in [kv, d1, r2, queue] {
        Worker::load_config(Arc::new(WorkerConfig::new(config)))
            .expect("a stored legacy product binding retains its isolated registration");
    }
}

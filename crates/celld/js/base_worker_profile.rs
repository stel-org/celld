//! Reviewed Base Worker runtime APIs installed only by the `BaseWorker`
//! profile.  This module must not be used by celld's `Default` bootstrap.
//!
//! The initial V0 slice is Deno's `Deno.memoryUsage()`, adapted from Base
//! Worker's `runtime/js/90_deno_ns.js` and `ext/os/lib.rs`. It uses celld's
//! current isolate and a lexical V8 callback rather than deno_core's op
//! registry. No filesystem, environment, network, subprocess, storage, or
//! control capability is exposed to application JavaScript.
use super::*;

const SOURCE: &str = include_str!("base_worker_profile.js");

/// Install the reviewed Base Worker API subset after celld's shared bootstrap
/// and before application-module evaluation.
pub(super) fn install(scope: &mut v8::PinScope) -> Result<()> {
    let op_name = v8::String::new(scope, "__base_worker_memory_usage")
        .ok_or_else(|| anyhow!("create Base Worker memory-usage operation name"))?;
    let op = v8::Function::new(scope, op_memory_usage)
        .ok_or_else(|| anyhow!("create Base Worker memory-usage operation"))?;
    let source = v8::String::new(scope, SOURCE)
        .ok_or_else(|| anyhow!("create Base Worker profile bootstrap source"))?;
    let arguments = InternalArguments {
        names: vec![op_name],
        values: vec![op.into()],
    };
    bootstrap::run_internal_script(scope, None, source, &arguments)?;
    // This existing feasibility regression remains intentionally test-only.
    // The shipping BaseWorker profile exposes only reviewed APIs from SOURCE.
    #[cfg(test)]
    bootstrap::install_base_worker_profile_probe(scope)?;
    Ok(())
}

/// Adapter for Base Worker's `op_runtime_memory_usage`. The callback is a
/// lexical bootstrap capability, so default Workers cannot name or call it.
fn op_memory_usage(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut return_value: v8::ReturnValue<v8::Value>,
) {
    let statistics = scope.get_heap_statistics();
    let rss = match current_process_rss_bytes() {
        Ok(rss) => rss,
        Err(()) => {
            let message = v8::String::new(
                scope,
                "Deno.memoryUsage is unavailable on this Base Worker host platform",
            )
            .expect("static Base Worker memory-usage error is valid V8 text");
            scope.throw_exception(v8::Exception::error(scope, message));
            return;
        }
    };
    // JavaScript numbers exactly represent the practical V8/process memory
    // range on the supported Linux host. The source contains only host-derived
    // unsigned decimal values, so it is safe to hand to JSON.parse.
    let value = format!(
        r#"{{"rss":{rss},"heapTotal":{},"heapUsed":{},"external":{}}}"#,
        statistics.total_heap_size(),
        statistics.used_heap_size(),
        statistics.external_memory(),
    );
    let value = v8::String::new(scope, &value)
        .expect("Base Worker memory-usage JSON contains valid V8 text");
    return_value.set(value.into());
}

/// Return this process's current resident set size without exposing a host
/// path or filesystem capability to Worker JavaScript. Linux is Base V0's
/// supported server platform; other hosts fail explicitly instead of returning
/// a misleading substitute such as maximum resident size.
#[cfg(target_os = "linux")]
fn current_process_rss_bytes() -> Result<u64, ()> {
    let status = std::fs::read_to_string("/proc/self/status").map_err(|_| ())?;
    let kibibytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_ascii_whitespace().next())
        .ok_or(())?
        .parse::<u64>()
        .map_err(|_| ())?;
    kibibytes.checked_mul(1024).ok_or(())
}

#[cfg(not(target_os = "linux"))]
fn current_process_rss_bytes() -> Result<u64, ()> {
    Err(())
}

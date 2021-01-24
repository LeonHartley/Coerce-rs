use opentelemetry::global;
use std::collections::HashMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub fn extract_trace_identifier(span: &Span) -> String {
    let mut headers = HashMap::<String, String>::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&span.context(), &mut headers)
    });

    let trace_id = headers
        .get("traceparent")
        .map_or_else(|| String::default(), |t| t.to_owned());

    trace_id
}

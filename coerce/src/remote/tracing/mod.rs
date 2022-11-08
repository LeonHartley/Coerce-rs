// use opentelemetry::trace::{TraceContextExt, TraceFlags};
// use tracing::Span;
// use tracing_opentelemetry::OpenTelemetrySpanExt;
//
// #[inline]
// pub fn extract_trace_identifier(span: &Span) -> String {
//     let context = span.context();
//     let span = context.span();
//     let span_context = span.span_context();
//     format!(
//         "{:02x}-{:032x}-{:016x}-{:02x}",
//         0,
//         span_context.trace_id(),
//         span_context.span_id(),
//         span_context.trace_flags() & TraceFlags::SAMPLED
//     )
// }

use std::collections::HashMap;

pub struct TracesHandleResult {
    pub spans_count: u64,
    pub spans: Vec<Span>,
}

impl TracesHandleResult {
    pub fn new() -> Self {
        TracesHandleResult { spans_count: 0, spans: vec![] }
    }

    pub fn handle_span(&mut self, span: Span) {
        self.spans_count += 1;
        self.spans.push(span);
    }
}

pub type TraceId = Vec<u8>;
pub type SpanId = Vec<u8>;

pub struct Span {
    pub name: String,
    pub id: SpanId,
    pub trace_id: TraceId,
    pub parent_span_id: Option<SpanId>,
    pub attributes: HashMap<String, String>,
}

pub struct Trace {
    pub span_ids: Vec<SpanId>,
}

impl Trace {
    pub fn new() -> Self {
        Trace { span_ids: vec![] }
    }
}
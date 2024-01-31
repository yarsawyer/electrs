use std::{sync::atomic::AtomicU64, time::Instant};

use tracing::info;
use tracing_indicatif::span_ext::IndicatifSpanExt;

pub struct Progress {
    span: tracing::Span,
    msg: String,
    start: Instant,
    c: AtomicU64,
    len: u64,
}

impl Progress {
    pub fn begin(msg: impl ToString, len: u64, c: u64) -> Self {
        let span = tracing::info_span!("");
        let _ = span.enter();
        span.pb_set_style(
            &indicatif::ProgressStyle::with_template("{prefix:.bold} <{bar}> {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        span.pb_set_length(len);
        span.pb_inc(c);
        let start = Instant::now();
        Self {
            span,
            msg: msg.to_string(),
            start,
            len,
            c: c.into(),
        }
    }
    pub fn inc(&self, c: u64) {
        self.span.pb_inc(c);
        self.c.fetch_add(c, std::sync::atomic::Ordering::AcqRel);
        self.update_msg();
    }
    fn update_msg(&self) {
        let time = self.start.elapsed().as_secs_f32();
        self.span.pb_set_message(&format!(
            "[{}/{}] {} | {time:.2} s",
            self.c.load(std::sync::atomic::Ordering::Acquire),
            self.len,
            &self.msg
        ));
    }
}
impl Drop for Progress {
    fn drop(&mut self) {
        let time = self.start.elapsed().as_secs_f32();
        info!("✔️ {} | {time:.2} s", &self.msg);
    }
}

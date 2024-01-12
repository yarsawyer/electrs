
pub struct NotFilter<A>(pub A);
pub struct AndFilter<A,B>(pub A, pub B);
pub struct OrFilter<A,B>(pub A, pub B);

impl<A: tracing_subscriber::layer::Filter<S>, B: tracing_subscriber::layer::Filter<S>, S> tracing_subscriber::layer::Filter<S> for AndFilter<A,B> {
    #[inline]
    fn enabled(&self, meta: &tracing::Metadata<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.0.enabled(meta, cx) && self.1.enabled(meta, cx)
    }

    fn callsite_enabled(&self, meta: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
        let a = self.0.callsite_enabled(meta);
        if a.is_never() { return a; }
        let b = self.1.callsite_enabled(meta);
        if !b.is_always() { return b; }
        a
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        // If either hint is `None`, return `None`. Otherwise, return the most restrictive.
        std::cmp::min(self.0.max_level_hint(), self.1.max_level_hint())
    }

    #[inline]
    fn event_enabled(&self, event: &tracing::Event<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.0.event_enabled(event, cx) && self.1.event_enabled(event, cx)
    }

    #[inline]
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_new_span(attrs, id, ctx.clone());
        self.1.on_new_span(attrs, id, ctx)
    }

    #[inline]
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_record(id, values, ctx.clone());
        self.1.on_record(id, values, ctx);
    }

    #[inline]
    fn on_enter(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_enter(id, ctx.clone());
        self.1.on_enter(id, ctx);
    }

    #[inline]
    fn on_exit(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_exit(id, ctx.clone());
        self.1.on_exit(id, ctx);
    }

    #[inline]
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_close(id.clone(), ctx.clone());
        self.1.on_close(id, ctx);
    }
}
impl<A: tracing_subscriber::layer::Filter<S>, B: tracing_subscriber::layer::Filter<S>, S> tracing_subscriber::layer::Filter<S> for OrFilter<A,B> {
    #[inline]
    fn enabled(&self, meta: &tracing::Metadata<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.0.enabled(meta, cx) || self.1.enabled(meta, cx)
    }

    fn callsite_enabled(&self, meta: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
        let a = self.0.callsite_enabled(meta);
        let b = self.1.callsite_enabled(meta);

        // If either filter will always enable the span or event, return `always`.
        if a.is_always() || b.is_always() {
            return tracing::subscriber::Interest::always();
        }

        // Okay, if either filter will sometimes enable the span or event,
        // return `sometimes`.
        if a.is_sometimes() || b.is_sometimes() {
            return tracing::subscriber::Interest::sometimes();
        }

        debug_assert!(
            a.is_never() && b.is_never(),
            "if neither filter was `always` or `sometimes`, both must be `never` (a={:?}; b={:?})",
            a,
            b,
        );
        tracing::subscriber::Interest::never()
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        // If either hint is `None`, return `None`. Otherwise, return the most restrictive.
        Some(std::cmp::max(self.0.max_level_hint()?, self.1.max_level_hint()?))
    }

    #[inline]
    fn event_enabled(&self, event: &tracing::Event<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.0.event_enabled(event, cx) && self.1.event_enabled(event, cx)
    }

    #[inline]
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_new_span(attrs, id, ctx.clone());
        self.1.on_new_span(attrs, id, ctx)
    }

    #[inline]
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_record(id, values, ctx.clone());
        self.1.on_record(id, values, ctx);
    }

    #[inline]
    fn on_enter(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_enter(id, ctx.clone());
        self.1.on_enter(id, ctx);
    }

    #[inline]
    fn on_exit(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_exit(id, ctx.clone());
        self.1.on_exit(id, ctx);
    }

    #[inline]
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_close(id.clone(), ctx.clone());
        self.1.on_close(id, ctx);
    }
}
impl<A: tracing_subscriber::layer::Filter<S>, S> tracing_subscriber::layer::Filter<S> for NotFilter<A> {
    #[inline]
    fn enabled(&self, meta: &tracing::Metadata<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        !self.0.enabled(meta, cx)
    }

    fn callsite_enabled(&self, meta: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
        match self.0.callsite_enabled(meta) {
            i if i.is_always() => tracing::subscriber::Interest::never(),
            i if i.is_never() => tracing::subscriber::Interest::always(),
            _ => tracing::subscriber::Interest::sometimes(),
        }
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        None
    }

    #[inline]
    fn event_enabled(&self, _: &tracing::Event<'_>, _: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        true
    }

    #[inline]
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_new_span(attrs, id, ctx);
    }

    #[inline]
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_record(id, values, ctx);
    }

    #[inline]
    fn on_enter(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_enter(id, ctx);
    }

    #[inline]
    fn on_exit(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_exit(id, ctx);
    }

    #[inline]
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.on_close(id, ctx);
    }
}


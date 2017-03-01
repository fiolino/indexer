package org.fiolino.indexer.sinks.builders;

import org.fiolino.common.processing.sink.Sink;
import org.fiolino.indexer.sinks.builders.SinkBuilder;

/**
 * Created by kuli on 13.10.16.
 */
public abstract class WrappedSinkBuilder<T, U> implements SinkBuilder<T> {

    private final SinkBuilder<U> mainBuilder;

    public WrappedSinkBuilder(SinkBuilder<U> mainBuilder) {
        this.mainBuilder = mainBuilder;
    }

    protected abstract Sink<T> appendFirst(Sink<U> target);

    @Override
    public final Sink<T> createSink() {
        Sink<U> sink = mainBuilder.createSink();
        return appendFirst(sink);
    }
}

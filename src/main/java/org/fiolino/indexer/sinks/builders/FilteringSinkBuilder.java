package org.fiolino.indexer.sinks.builders;

import org.fiolino.common.processing.sink.FilteringSink;
import org.fiolino.common.processing.sink.Sink;

import java.util.function.Predicate;

/**
 * Created by kuli on 13.10.16.
 */
public class FilteringSinkBuilder<T> extends WrappedSinkBuilder<T, T> {
    private final Predicate<? super T> filter;

    public FilteringSinkBuilder(SinkBuilder<T> mainBuilder, Predicate<? super T> filter) {
        super(mainBuilder);
        this.filter = filter;
    }

    @Override
    protected Sink<T> appendFirst(Sink<T> target) {
        return new FilteringSink<>(target, filter);
    }
}

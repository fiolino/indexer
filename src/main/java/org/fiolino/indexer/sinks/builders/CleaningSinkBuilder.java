package org.fiolino.indexer.sinks.builders;

import org.fiolino.common.container.Container;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.ModifyingSink;
import org.fiolino.common.processing.sink.Sink;
import org.fiolino.indexer.DeleteStrategy;

import java.util.function.Function;

/**
 * Builds a sink that cleans out dormant items from the index.
 * <p>
 * Created by kuli on 14.10.16.
 */
public class CleaningSinkBuilder<T> extends WrappedSinkBuilder<T, T> {
    private final Cleaner cleaner;
    private final Selector<DeleteStrategy> selector;
    private final Function<? super T, ?> idFetcher;

    public CleaningSinkBuilder(SinkBuilder<T> mainBuilder, Cleaner cleaner, Selector<DeleteStrategy> selector, Function<? super T, ?> idFetcher) {
        super(mainBuilder);
        this.cleaner = cleaner;
        this.selector = selector;
        this.idFetcher = idFetcher;
    }

    @Override
    protected Sink<T> appendFirst(Sink<T> target) {
        return new CleaningSink<>(target, idFetcher);
    }

    /**
     * This sink collects all IDs of the documents that get uploaded.
     * IDs missing here will be deleted later. This behaviour makes sure that documents that get filtered by our
     * accept() method are subject to deletion.
     */
    private class CleaningSink<T> extends ModifyingSink<T> {

        private final Function<? super T, ?> idFetcher;

        public CleaningSink(Sink<? super T> target, Function<? super T, ?> idFetcher) {
            super(target);
            this.idFetcher = idFetcher;
        }

        @Override
        protected void touch(T element, Container metadata) throws Exception {
            DeleteStrategy deleteStrategy = selector.get(metadata);
            deleteStrategy.accept(idFetcher.apply(element));
        }

        @Override
        public void commit(Container metadata) throws Exception {
            DeleteStrategy deleteStrategy = selector.get(metadata);
            deleteStrategy.deleteRemaining(cleaner);
            super.commit(metadata);
        }
    }
}

package org.fiolino.indexer.sinks;

import org.fiolino.common.container.Container;
import org.fiolino.common.processing.sink.ChainedSink;
import org.fiolino.common.processing.sink.CloneableSink;
import org.fiolino.common.processing.sink.Sink;

/**
 * A sink that splits into two different targets, depending on the type of the incoming object.
 * <p>
 * Created by kuli on 15.06.16.
 */
public final class UpdatePairSplitter<T, U extends T, V extends T>
        implements CloneableSink<UpdatePair<T>, UpdatePairSplitter<T, U, V>> {
    private final Class<U> firstCheck;
    private final Sink<? super UpdatePair<U>> firstSink;
    private final Class<V> secondCheck;
    private final Sink<? super UpdatePair<V>> secondSink;

    public UpdatePairSplitter(Class<U> firstCheck, Sink<? super UpdatePair<U>> firstSink,
                              Class<V> secondCheck, Sink<? super UpdatePair<V>> secondSink) {
        if (firstCheck.isAssignableFrom(secondCheck)) {
            throw new IllegalArgumentException(secondCheck.getName() + " mustn't be related to " + firstCheck.getName());
        }
        this.firstCheck = firstCheck;
        this.firstSink = firstSink;
        this.secondCheck = secondCheck;
        this.secondSink = secondSink;
    }

    @Override
    public void accept(UpdatePair<T> value, Container metadata) throws Exception {
        if (value.isOf(firstCheck)) {
            firstSink.accept(value.as(firstCheck), metadata);
            return;
        }
        if (value.isOf(secondCheck)) {
            secondSink.accept(value.as(secondCheck), metadata);
            return;
        }

        throw new IllegalArgumentException(value + " must be of either " + firstCheck.getName() + " or "
                + secondCheck.getName());
    }

    @Override
    public void commit(Container metadata) throws Exception {
        firstSink.commit(metadata);
        secondSink.commit(metadata);
    }

    @Override
    public void partialCommit(Container metadata) throws Exception {
        if (firstSink instanceof CloneableSink) {
            ((CloneableSink<?, ?>) firstSink).partialCommit(metadata);
        }
        if (secondSink instanceof CloneableSink) {
            ((CloneableSink<?, ?>) secondSink).partialCommit(metadata);
        }
    }

    @Override
    public UpdatePairSplitter<T, U, V> createClone() {
        return new UpdatePairSplitter<>(firstCheck, ChainedSink.targetForCloning(firstSink),
                secondCheck, ChainedSink.targetForCloning(secondSink));
    }
}

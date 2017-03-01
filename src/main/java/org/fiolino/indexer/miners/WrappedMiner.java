package org.fiolino.indexer.miners;

import org.fiolino.common.container.Container;
import org.fiolino.common.processing.sink.Sink;

/**
 * Created by Kuli on 14/10/2016.
 */
public abstract class WrappedMiner<T> implements Miner<T> {
    private final Miner<? extends T> target;

    public WrappedMiner(Miner<? extends T> target) {
        this.target = target;
    }

    @Override
    public void digIDsInto(Sink<? super T> targetSink, Container metadata, Object[] ids) throws Exception {
        target.digIDsInto(targetSink, metadata, ids);
    }

    @Override
    public void digAllInto(Sink<? super T> targetSink, Container metadata) throws Exception {
        target.digAllInto(targetSink, metadata);
    }

    @Override
    public String toString() {
        return "Wrapped " + target;
    }
}

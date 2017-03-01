package org.fiolino.indexer.miners;

import org.fiolino.common.container.Container;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.Sink;
import org.fiolino.indexer.DeleteStrategy;
import org.fiolino.indexer.RemainingIDsDeleteStrategy;
import org.fiolino.indexer.TimestampDeleteStragegy;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Kuli on 14/10/2016.
 */
public class DeleteStrategyInjector<T> extends WrappedMiner<T> {
    private final Selector<Long> timestampSelector;
    private final Selector<DeleteStrategy> deleteStrategySelector;

    public DeleteStrategyInjector(Miner<? extends T> target,
                                  Selector<Long> timestampSelector,
                                  Selector<DeleteStrategy> deleteStrategySelector) {
        super(target);
        this.timestampSelector = timestampSelector;
        this.deleteStrategySelector = deleteStrategySelector;
    }

    @Override
    public void digAllInto(Sink<? super T> targetSink, Container metadata) throws Exception {
        long startTime = System.currentTimeMillis();
        metadata.set(timestampSelector, startTime);
        metadata.set(deleteStrategySelector, new TimestampDeleteStragegy(startTime));

        super.digAllInto(targetSink, metadata);
    }

    @Override
    public void digIDsInto(Sink<? super T> targetSink, Container metadata, Object[] ids) throws Exception {
        long startTime = System.currentTimeMillis();
        metadata.set(timestampSelector, startTime);
        Set<Object> idSet = new HashSet<>(ids.length * 2);
        Collections.addAll(idSet, ids);
        metadata.set(deleteStrategySelector, new RemainingIDsDeleteStrategy(idSet));

        super.digIDsInto(targetSink, metadata, ids);
    }
}

package org.fiolino.indexer.sinks.builders;

import org.fiolino.common.processing.sink.Sink;

/**
 * Builds the sink used for the Miner.
 * <p>
 * Created by Kuli on 07/10/2016.
 */
public interface SinkBuilder<T> {

    /**
     * The factory method. Will be called usually only once.
     */
    Sink<T> createSink();
}

package org.fiolino.indexer.miners;

import org.fiolino.common.container.Container;
import org.fiolino.common.processing.sink.Sink;

/**
 * A miner digs for data and throws it into some sink.
 * <p>
 * Created by kuli on 05.10.16.
 */
public interface Miner<T> {

    /**
     * Digs for specific IDs. Commits after being finished.
     *
     * @param targetSink Where to put the data
     * @param metadata   The metadata for the sink
     * @param ids        The ids to look for
     */
    void digIDsInto(Sink<? super T> targetSink, Container metadata, Object[] ids) throws Exception;

    /**
     * Digs all data into the sink.
     *
     * @param targetSink Where to put the data
     * @param metadata   The metadata for the sink
     */
    void digAllInto(Sink<? super T> targetSink, Container metadata) throws Exception;
}

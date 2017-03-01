package org.fiolino.indexer;

/**
 * Created by kuli on 28.12.15.
 */
public interface Indexer<T> {

    /**
     * Indexes all of this type.
     *
     * @param ids The given ids, or none of everything should be indexed.
     */
    void index(Long... ids) throws IndexerException;

    void index(T... items) throws IndexerException;
}

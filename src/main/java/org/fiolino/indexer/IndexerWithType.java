package org.fiolino.indexer;

/**
 * Created by Kuli on 25/10/2016.
 */
public final class IndexerWithType {
    private final String searchableType;
    private final Indexer<?> indexer;

    public IndexerWithType(String searchableType, Indexer<?> indexer) {
        this.searchableType = searchableType;
        this.indexer = indexer;
    }

    public String getSearchableType() {
        return searchableType;
    }

    public Indexer<?> getIndexer() {
        return indexer;
    }
}

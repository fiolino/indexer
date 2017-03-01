/**
 * Copyright (C) 2015 Market Logic Software
 * All rights reserved
 */
package org.fiolino.indexer;

import org.apache.solr.client.solrj.SolrClient;
import org.fiolino.common.container.Schema;
import org.fiolino.common.container.Selector;
import org.fiolino.common.ioc.Beans;
import org.fiolino.common.processing.sink.Sink;
import org.fiolino.data.annotation.Category;
import org.fiolino.indexer.miners.DeleteStrategyInjector;
import org.fiolino.indexer.miners.Miner;
import org.fiolino.indexer.sinks.builders.*;
import org.fiolino.searcher.Realm;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by kuli on 28.12.15.
 */
public class IndexerFactory<T> {

    /**
     * On which domain this is running.
     */
    protected final Realm realm;

    protected final SolrClient solrClient;

    protected final Class<T> type;
    protected final Function<? super T, ?> idFetcher;

    protected final Schema schema;
    protected final Selector<Long> timestampSelector;
    protected final Selector<DeleteStrategy> deleteStrategySelector;

    private Miner<T> miner;

    private final List<Predicate<? super T>> filters = new ArrayList<>();

    public IndexerFactory(Realm realm, Class<T> type, Function<? super T, ?> idFetcher) {
        this.realm = realm;
        this.type = type;
        this.idFetcher = idFetcher;
        schema = new Schema(type.getSimpleName());
        timestampSelector = schema.createSelector();
        deleteStrategySelector = schema.createSelector();
        solrClient = realm.getSolrClient();
    }

    public IndexerFactory<T> withFilter(Predicate<? super T> test) {
        filters.add(test);
        return this;
    }

    public static String getCategoryFrom(Class<?> type) {
        Category catAnnotation = type.getAnnotation(Category.class);
        if (catAnnotation != null) {
            return catAnnotation.value();
        }
        return type.getSimpleName();
    }

    public final Class<T> getType() {
        return type;
    }

    public void setMiner(Miner<T> miner) {
        this.miner = wrapWithDeleteStrategy(miner);
    }

    protected SinkBuilder<T> instantiatePlainSinkBuilder() {
        return new SolrSinkBuilder<>(type.getSimpleName(), type, solrClient, schema, timestampSelector);
    }

    protected Cleaner createCleaner() {
        return new DefaultCleaner(solrClient);
    }

    protected SinkBuilder<T> addCleaner(SinkBuilder<T> target) {
        Cleaner c = createCleaner();
        return new CleaningSinkBuilder<>(target, c, deleteStrategySelector, idFetcher);
    }

    /**
     * Add individual filters here.
     */
    protected SinkBuilder<T> addFiltersTo(SinkBuilder<T> target) {
        SinkBuilder<T> b = target;
        for (Predicate<? super T> f : filters) {
            b = new FilteringSinkBuilder<>(b, f);
        }
        return b;
    }

    protected SinkBuilder<T> createSinkBuilder() {
        SinkBuilder<T> main = instantiatePlainSinkBuilder();
        SinkBuilder<T> withCleaner = addCleaner(main);
        return addFiltersTo(withCleaner);
    }

    protected Sink<T> createSink() {
        SinkBuilder<T> b = createSinkBuilder();
        return b.createSink();
    }

    protected Miner<T> instantiatePlainMiner() {
        IndexerProperties props = Beans.get(IndexerProperties.class);

        return null;
    }

    protected Miner<T> wrapWithDeleteStrategy(Miner<T> miner) {
        return new DeleteStrategyInjector<>(miner, timestampSelector, deleteStrategySelector);
    }

    protected Miner<T> createMiner() {
        Miner<T> miner = instantiatePlainMiner();
        return wrapWithDeleteStrategy(miner);
    }

    public Indexer<T> createIndexer() {
        if (miner == null) {
            miner = createMiner();
        }
        return new MiningIndexer<>(schema, miner, createSink());
    }

    @Override
    public String toString() {
        return getClass().getName() + " (" + type.getName() + ") <" + realm + ">";
    }
}

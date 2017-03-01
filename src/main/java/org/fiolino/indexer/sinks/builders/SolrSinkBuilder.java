package org.fiolino.indexer.sinks.builders;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.analyzing.ModelInconsistencyException;
import org.fiolino.common.container.Schema;
import org.fiolino.common.container.Selector;
import org.fiolino.common.ioc.Beans;
import org.fiolino.common.processing.Analyzer;
import org.fiolino.common.processing.BeanCreator;
import org.fiolino.common.processing.ModelDescription;
import org.fiolino.common.processing.sink.AggregatingSink;
import org.fiolino.common.processing.sink.CreatingSink;
import org.fiolino.common.processing.sink.ParallelizingSink;
import org.fiolino.common.processing.sink.Sink;
import org.fiolino.indexer.IndexerProperties;
import org.fiolino.indexer.SolrDocumentFiller;
import org.fiolino.indexer.sinks.SolrSink;
import org.fiolino.indexer.sinks.TimestampSetter;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by kuli on 28.12.15.
 */
public class SolrSinkBuilder<T> implements SinkBuilder<T> {

    /**
     * This executor is used in the parallel sink. There should be only one thread pool,
     * so this is static.
     */
    public static final Executor indexerExecutor = Executors.newCachedThreadPool((r) ->
            new Thread(r, "Indexer thread"));

    /**
     * The indexed type. Should be some kind of domain model.
     */
    private final Class<T> type;

    /**
     * The Solr client library.
     */
    private final SolrClient solrClient;

    /**
     * On which domain this is running.
     */
    private final String name;

    /**
     * Container for properties.
     */
    private final IndexerProperties indexerProperties;

    /**
     * This is used for uploading data.
     */
    private final Schema schema;
    private final Selector<Long> timestampSelector;

    public SolrSinkBuilder(String name, Class<T> type, SolrClient solrClient,
                           Schema schema, Selector<Long> timestampSelector) {
        indexerProperties = Beans.get(IndexerProperties.class);
        this.name = name;
        this.type = type;
        this.solrClient = solrClient;
        this.schema = schema;
        this.timestampSelector = timestampSelector;
    }

    protected final IndexerProperties getIndexerProperties() {
        return indexerProperties;
    }

    protected final Class<T> getType() {
        return type;
    }

    protected final SolrClient getSolrClient() {
        return solrClient;
    }

    /**
     * Builds the factory for {@link SolrInputDocument}s.
     *
     * @param type The input type
     */
    protected static <T> BeanCreator<T, ? extends SolrInputDocument> buildSolrDocumentFactory(Class<T> type) {
        ModelDescription description = new ModelDescription(type, SolrDocumentFactoryBuilder.SCHEMA.createContainer());
        SolrDocumentFactoryBuilder<T> factoryBuilder = new SolrDocumentFactoryBuilder<>();
        try {
            Analyzer.analyzeAll(description, factoryBuilder);
        } catch (ModelInconsistencyException ex) {
            throw new AssertionError(ex);
        }
        SolrDocumentFiller<T> builderFunction = factoryBuilder.getFiller();
        return BeanCreator.using(SolrInputDocument::new, builderFunction.asProcessor());
    }

    protected final Schema getSchema() {
        return schema;
    }

    /**
     * Creates a sink that sets the timestamp to a Solr document.
     * The timestamp is used to delete expired content in case of a full re-index.
     */
    protected TimestampSetter createTimestampSetter(Sink<SolrInputDocument> target) {
        return new TimestampSetter(target, timestampSelector);
    }

    /**
     * This is a hook to add additional content to an uploaded document.
     */
    protected Sink<SolrInputDocument> addAdditionalContentTo(Sink<SolrInputDocument> original) {
        return original;
    }

    protected Sink<T> addAdditionalModelSink(Sink<T> sink) {
        return sink;
    }

    /**
     * Creates the sink.
     * <p>
     * This is called on demand instead of the constructor to make it easier to overwrite the method in subclasses.
     * <p>
     * Subclasses may ,also like to overwrite one of the hook methods; this rather is a skeleton wrapper.
     */
    @Override
    public Sink<T> createSink() {
        SolrSink solrSink = createSolrSink();

        CreatingSink<T, SolrInputDocument> sink = createSolrDocSink(solrSink, getType());

        Sink<T> additional = addAdditionalModelSink(sink);

        return parallelize(additional, solrSink);
    }

    /**
     * Creates the sink that transforms the full domain object to a final SolrInputDocument.
     * It also adds timestamp data to it.
     */
    protected <X> CreatingSink<X, SolrInputDocument> createSolrDocSink(SolrSink sink, Class<X> type) {
        Sink<SolrInputDocument> docTarget = new AggregatingSink<>(sink, indexerProperties.getUploadChunkSize());

        Sink<SolrInputDocument> optional = addAdditionalContentTo(docTarget);
        TimestampSetter timestampSetter = createTimestampSetter(optional);

        BeanCreator<X, ? extends SolrInputDocument> beanCreator = buildSolrDocumentFactory(type);
        return new CreatingSink<>(timestampSetter, beanCreator,
                indexerProperties.getFactoryTimeout(), TimeUnit.MILLISECONDS);
    }

    protected SolrSink createSolrSink() {
        return new SolrSink(solrClient, schema, indexerProperties.getUpdatesBeforeCommit());
    }

    /**
     * Creates a sink that parallelizes the upload.
     */
    protected Sink<T> parallelize(Sink<T> target, SolrSink solrSink) {
        return ParallelizingSink.createFor(target, sinkName(), indexerExecutor::execute,
                indexerProperties.getSolrThreads(), getQueueSize());
    }

    /**
     * The sink name of the parallel task. Used for debugging and logging.
     */
    protected String sinkName() {
        return name;
    }

    /**
     * The size of the parallel queue for each thread.
     */
    protected int getQueueSize() {
        return 200;
    }

    @Override
    public String toString() {
        return getClass().getName() + " (" + name + ")";
    }
}

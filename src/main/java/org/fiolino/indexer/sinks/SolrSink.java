package org.fiolino.indexer.sinks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.container.Container;
import org.fiolino.common.container.Schema;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.ThreadsafeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael Kuhlmann on 22.12.2015.
 */
@ThreadSafe
public class SolrSink implements ThreadsafeSink<List<SolrInputDocument>> {

    private static final Logger logger = LoggerFactory.getLogger(IndexerService.class.getName());

    private final SolrClient solrClient;

    private final int updatesBeforeCommit;

    private final Selector<AtomicInteger> docCounter;
    private final AtomicInteger commitCounter = new AtomicInteger();

    public SolrSink(SolrClient solrClient, Schema schema, int updatesBeforeCommit) {
        this.solrClient = solrClient;
        this.updatesBeforeCommit = updatesBeforeCommit;
        docCounter = schema.createLazilyInitializedSelector(AtomicInteger::new);
    }

    @Override
    public void accept(List<SolrInputDocument> docs, Container metadata) throws IOException, SolrServerException {
        if (docs.isEmpty()) {
            // no docs specified
            return;
        }

        solrClient.add(docs);
        int n = docs.size();
        int updateCount = commitCounter.addAndGet(n);
        while (updateCount >= updatesBeforeCommit) {
            if (commitCounter.compareAndSet(updateCount, 0)) {
                AtomicInteger c = metadata.get(docCounter);
                c.getAndAdd(updateCount);

                // fix maxWarmingSearchers exception (BM-10630)
                int count = 0;
                do {
                    try {
                        solrClient.commit();
                    } catch (RemoteSolrException e) {
                        if (e.getMessage().contains("try again later")) {
                            try {
                                logger.warn("Got exception from solr and will try commit later...", e);
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e1) {
                                // do nothing
                                Thread.currentThread().interrupt();
                                break;
                            }
                            continue;
                        }
                        throw e;
                    }
                    break;
                } while (count++ < 10);

                return;
            }
            updateCount = commitCounter.get();
        }
    }

    @Override
    public void commit(Container metadata) throws IOException, SolrServerException {
        int count = commitCounter.getAndSet(0);
        if (count > 0) {
            solrClient.commit();
        }
        AtomicInteger c = metadata.remove(docCounter);
        if (c != null) {
            count += c.get();
        }
        if (count > 0) {
            logger.info("Uploaded " + count + " docs to Solr.");
        }
    }
}

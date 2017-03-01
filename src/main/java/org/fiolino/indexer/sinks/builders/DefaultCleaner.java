package org.fiolino.indexer.sinks.builders;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.fiolino.indexer.sinks.TimestampSetter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by kuli on 07.10.16.
 */
public class DefaultCleaner implements Cleaner {
    protected final SolrClient solrClient;

    public DefaultCleaner(SolrClient solrClient) {
        this.solrClient = solrClient;
    }

    @Override
    public void deleteByTimestamp(long everythingBeforeThis) throws SolrServerException, IOException {
        long timestamp = everythingBeforeThis - TimeUnit.SECONDS.toMillis(10);
        solrClient.deleteByQuery(TimestampSetter.TIMESTAMP_FIELD + ":[0 TO " + timestamp + "}");
    }

    @Override
    public void deleteByIDs(List<String> idList) throws SolrServerException, IOException {
        solrClient.deleteById(idList);
    }
}

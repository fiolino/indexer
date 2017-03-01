package org.fiolino.indexer.sinks.builders;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;

/**
 * Created by kuli on 07.10.16.
 */
public interface Cleaner {

    /**
     * Callback from the timestamp based delete strategy.
     */
    void deleteByTimestamp(long startTime) throws SolrServerException, IOException;

    /**
     * Callback from the id based delete strategy.
     */
    void deleteByIDs(List<String> idList) throws SolrServerException, IOException;
}

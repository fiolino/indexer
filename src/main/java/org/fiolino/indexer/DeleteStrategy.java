package org.fiolino.indexer;

import java.io.IOException;

import org.fiolino.indexer.sinks.builders.Cleaner;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * Created by Michael Kuhlmann on 14.01.2016.
 */
public interface DeleteStrategy {

    /**
     * Accepts an entry, which won't be deleted then.
     */
    void accept(Object id);

    /**
     * Will be called when indexing is done.
     *
     * @param cleaner The cleaning strategy
     * @return If something was deleted
     * @throws SolrServerException In case of failed server call
     * @throws IOException         In case of failed server call
     */
    boolean deleteRemaining(Cleaner cleaner) throws SolrServerException, IOException;
}

package org.fiolino.indexer;

import java.io.IOException;

import org.fiolino.indexer.sinks.builders.Cleaner;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * Created by Michael Kuhlmann on 14.01.2016.
 */
public class TimestampDeleteStragegy implements DeleteStrategy {

    private final long startTime;

    public TimestampDeleteStragegy(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void accept(Object id) {
        //Nothing to do here
    }

    @Override
    public boolean deleteRemaining(Cleaner cleaner) throws SolrServerException, IOException {
        cleaner.deleteByTimestamp(startTime);
        return true;
    }
}

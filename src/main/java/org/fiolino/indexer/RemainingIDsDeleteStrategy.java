package org.fiolino.indexer;

import org.fiolino.indexer.sinks.builders.Cleaner;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Michael Kuhlmann on 14.01.2016.
 */
public class RemainingIDsDeleteStrategy implements DeleteStrategy {

    private static final Logger logger = LoggerFactory.getLogger(RemainingIDsDeleteStrategy.class);

    private final Set<Object> idsToUpdate;

    public RemainingIDsDeleteStrategy(Set<Object> idsToUpdate) {
        this.idsToUpdate = idsToUpdate;
    }

    @Override
    public void accept(Object id) {
        idsToUpdate.remove(id);
    }

    @Override
    public boolean deleteRemaining(Cleaner cleaner) throws SolrServerException, IOException {
        if (idsToUpdate.isEmpty()) {
            return false;
        }
        List<String> idList = new ArrayList<>(idsToUpdate.size());
        for (Object id : idsToUpdate) {
            idList.add(makeStringID(id));
        }
        logger.info("Removing these unprocessed but requested ids: " + idList);
        cleaner.deleteByIDs(idList);
        return true;
    }

    private String makeStringID(Object id) {
        return "MLS_" + id;
    }
}

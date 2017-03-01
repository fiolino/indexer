package org.fiolino.indexer;

import org.fiolino.common.ioc.Component;
import org.fiolino.common.ioc.Property;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by kuli on 30.03.16.
 */
@Component
public class IndexerProperties {
    private final int rootChunkSize;
    private final int documentFetchSize;
    private final int uploadChunkSize;
    private final int updatesBeforeCommit;
    private final int solrThreads;
    private final long factoryTimeout;
    private final Set<String> ignoredAnalyticsLabels;

    public IndexerProperties(@Property(value = "org.fiolino.jdbc.windowSize", defaultValue = "500") int rootChunkSize,
                             @Property(value = "org.fiolino.document.fetchsize", defaultValue = "50") int documentFetchSize,
                             @Property(value = "org.fiolino.solr.pageSize", defaultValue = "100") int uploadChunkSize,
                             @Property(value = "org.fiolino.solr.updatesBeforeCommit", defaultValue = "1000") int updatesBeforeCommit,
                             @Property(value = "org.fiolino.solr.threads", defaultValue = "-1") int solrThreads,
                             @Property(value = "org.fiolino.analytics.ignored.labels", defaultValue = "") String ignoredLabelsString,
                             @Property(value = "org.fiolino.solr.warntime", defaultValue = "2500") long factoryTimeout) {
        this.rootChunkSize = rootChunkSize;
        this.documentFetchSize = documentFetchSize;
        this.uploadChunkSize = uploadChunkSize;
        this.updatesBeforeCommit = updatesBeforeCommit;
        this.solrThreads = solrThreads;
        this.factoryTimeout = factoryTimeout;
        HashSet<String> ignoredLabels = new HashSet<>();
        for (String l : ignoredLabelsString.split(",")) {
            ignoredLabels.add(l.toLowerCase());
        }
        this.ignoredAnalyticsLabels = Collections.unmodifiableSet(ignoredLabels);
    }

    public int getRootChunkSize() {
        return rootChunkSize;
    }

    public int getDocumentFetchSize() {
        return documentFetchSize;
    }

    public int getUploadChunkSize() {
        return uploadChunkSize;
    }

    public int getSolrThreads() {
        return solrThreads;
    }

    public int getUpdatesBeforeCommit() {
        return updatesBeforeCommit;
    }

    public Set<String> getIgnoredAnalyticsLabels() {
        return ignoredAnalyticsLabels;
    }

    public long getFactoryTimeout() {
        return factoryTimeout;
    }

    @Override
    public String toString() {
        return "Properties";
    }
}

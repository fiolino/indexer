package org.fiolino.indexer.sinks;

import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.container.Container;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.CloneableSink;
import org.fiolino.common.processing.sink.ModifyingSink;
import org.fiolino.common.processing.sink.Sink;

/**
 * Created by kuli on 04.01.16.
 */
public final class TimestampSetter extends ModifyingSink<SolrInputDocument>
        implements CloneableSink<SolrInputDocument, TimestampSetter> {

    public static final String TIMESTAMP_FIELD = "timestamp";
    private final Selector<Long> timestampSelector;

    public TimestampSetter(Sink<? super SolrInputDocument> target, Selector<Long> timestampSelector) {
        super(target);
        this.timestampSelector = timestampSelector;
    }

    @Override
    protected void touch(SolrInputDocument doc, Container metadata) {
        Long timestamp = metadata.get(timestampSelector);
        if (timestamp == null) {
            throw new IllegalStateException("No timestamp given!");
        }
        doc.addField(TIMESTAMP_FIELD, timestamp);
    }

    @Override
    public TimestampSetter createClone() {
        return new TimestampSetter(targetForCloning(), timestampSelector);
    }

    @Override
    public void partialCommit(Container metadata) throws Exception {
        if (getTarget() instanceof CloneableSink) {
            ((CloneableSink<?, ?>) getTarget()).partialCommit(metadata);
        }
    }
}

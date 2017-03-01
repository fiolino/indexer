package org.fiolino.indexer;

import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.container.Container;
import org.fiolino.common.container.Schema;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.NullSink;
import org.fiolino.indexer.sinks.TimestampSetter;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSetterTest {

    @Test
    public void testTimestampSetter() throws Throwable {
        Schema schema = new Schema("Test");
        Selector<Long> selector = schema.createSelector();
        TimestampSetter timestampSetter = new TimestampSetter(new NullSink<>(), selector);

        SolrInputDocument doc = new SolrInputDocument();
        Container container = schema.createContainer();
        long startTime = System.currentTimeMillis();
        container.set(selector, startTime);

        timestampSetter.accept(doc, container);

        Assert.assertTrue(doc.containsKey(TimestampSetter.TIMESTAMP_FIELD));

        long time = ((Long) doc.get(TimestampSetter.TIMESTAMP_FIELD).getValue());

        Assert.assertEquals(startTime, time);
    }
}

package org.fiolino.indexer;

import com.mls.common.test.categories.UnitTest;
import com.mls.core.common.container.Container;
import com.mls.core.common.container.Schema;
import com.mls.core.common.container.Selector;
import com.mls.core.common.processing.sink.NullSink;
import org.fiolino.indexer.sinks.TimestampSetter;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class TimestampSetterTest {

	@Test
	public void testTimestampSetter() throws Throwable {
    Schema schema = new Schema("Test");
    Selector<Long> selector = schema.createSelector();
    TimestampSetter timestampSetter = new TimestampSetter(new NullSink<SolrInputDocument>(), selector);

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

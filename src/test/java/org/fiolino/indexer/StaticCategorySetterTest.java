package org.fiolino.indexer;

import com.mls.common.test.categories.UnitTest;
import com.mls.core.common.container.Container;
import com.mls.core.common.processing.sink.NullSink;
import org.fiolino.indexer.sinks.StaticCategorySetter;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class StaticCategorySetterTest {

	@Test
	public void testStaticCategorySetter() throws Throwable {
		StaticCategorySetter categorySetter = new StaticCategorySetter(new NullSink<SolrInputDocument>(), "cat1");

		SolrInputDocument doc = new SolrInputDocument();

		categorySetter.accept(doc, Container.empty());

		Assert.assertTrue(doc.containsKey(StaticCategorySetter.CATEGORY_FIELD));

		String cat = ((String) doc.get(StaticCategorySetter.CATEGORY_FIELD).getValue());

		Assert.assertEquals("cat1", cat);
	}

}

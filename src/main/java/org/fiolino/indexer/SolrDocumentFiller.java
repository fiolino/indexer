package org.fiolino.indexer;

import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.processing.Processor;

/**
 * Created by kuli on 10.10.16.
 */
@FunctionalInterface
public interface SolrDocumentFiller<T> {

    void process(T model, SolrInputDocument doc, int count);

    default SolrDocumentFiller<T> andThen(SolrDocumentFiller<T> nextTask) {
        return new MultiFiller<>(this, nextTask);
    }

    default Processor<T, SolrInputDocument> asProcessor() {
        return (source, doc) -> SolrDocumentFiller.this.process(source, doc, 1);
    }

    class MultiFiller<T> implements SolrDocumentFiller<T> {
        private final SolrDocumentFiller<T> first;
        private final SolrDocumentFiller<T> second;

        MultiFiller(SolrDocumentFiller<T> first, SolrDocumentFiller<T> second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void process(T model, SolrInputDocument doc, int count) {
            first.process(model, doc, count);
            second.process(model, doc, count);
        }
    }

    class DoNothing<T> implements SolrDocumentFiller<T> {
        @Override
        public void process(T model, SolrInputDocument doc, int count) {
            // Do nothing, as the name says.
        }

        @Override
        public SolrDocumentFiller<T> andThen(SolrDocumentFiller<T> nextTask) {
            return nextTask;
        }
    }

    static <T> SolrDocumentFiller<T> doNothing() {
        return new DoNothing<>();
    }
}

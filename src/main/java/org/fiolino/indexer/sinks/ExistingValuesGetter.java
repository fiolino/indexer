package org.fiolino.indexer.sinks;

import org.fiolino.common.container.Container;
import org.fiolino.common.container.Selector;
import org.fiolino.common.processing.sink.ChainedSink;
import org.fiolino.common.processing.sink.Sink;
import org.fiolino.data.base.Identified;
import org.fiolino.indexer.DeleteStrategy;
import org.fiolino.searcher.Realm;
import org.fiolino.searcher.searcher.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Kuli on 4/20/2016.
 */
public class ExistingValuesGetter<T extends Identified> extends ChainedSink<List<T>, List<UpdatePair<T>>> {

    private static final Logger logger = LoggerFactory.getLogger(ExistingValuesGetter.class);

    private final Realm realm;
    private final Searcher<T> searcher;

    private final Selector<DeleteStrategy> deleteStrategySelector;

    public ExistingValuesGetter(Sink<List<UpdatePair<T>>> target, Realm realm, Searcher<T> searcher,
                                Selector<DeleteStrategy> deleteStrategySelector) {
        super(target);
        this.realm = realm;
        this.searcher = searcher;
        this.deleteStrategySelector = deleteStrategySelector;
    }

    @Override
    public void accept(List<T> values, Container metadata) throws Exception {
        Long[] ids = new Long[values.size()];
        int i = 0;
        for (T each : values) {
            ids[i++] = each.getId();
        }
        List<T> existing;
        try {
            existing = searcher.searchByIDs(realm, "id", ids);
        } catch (RuntimeException ex) {
            logger.error("Cannot load existing documents!", ex);
            getTarget().accept(combine(values, Collections.<Long, T>emptyMap()), metadata);
            return;
        }
        DeleteStrategy deleteStrategy = deleteStrategySelector.get(metadata);
        Map<Long, T> map = createIDMap(existing, deleteStrategy, ids);

        getTarget().accept(combine(values, map), metadata);
    }

    private List<UpdatePair<T>> combine(List<T> updated, Map<Long, T> existing) {
        List<UpdatePair<T>> result = new ArrayList<>(updated.size());
        for (T u : updated) {
            T x = existing.get(u.getId());
            result.add(UpdatePair.withExisting(u, x));
        }
        return result;
    }

    public static <T extends Identified> Map<Long, T> createIDMap(List<T> existing) {
        Map<Long, T> map = new HashMap<>(existing.size() * 2);
        for (T each : existing) {
            map.put(each.getId(), each);
        }
        return map;
    }

    public static <T extends Identified> Map<Long, T> createIDMap(List<T> existing,
                                                                  DeleteStrategy deleteStrategy,
                                                                  Long... ids) {
        Map<Long, T> map = createIDMap(existing);
        for (Long requested : ids) {
            if (!map.containsKey(requested)) {
                deleteStrategy.accept(requested);
            }
        }
        return map;
    }
}

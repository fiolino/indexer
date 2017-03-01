package org.fiolino.indexer.sinks.builders;

import org.apache.solr.common.SolrInputDocument;
import org.fiolino.common.FieldType;
import org.fiolino.common.analyzing.Analyzeable;
import org.fiolino.common.analyzing.AnnotationInterest;
import org.fiolino.common.analyzing.ModelInconsistencyException;
import org.fiolino.common.container.Container;
import org.fiolino.common.container.Schema;
import org.fiolino.common.container.Selector;
import org.fiolino.common.ioc.Beans;
import org.fiolino.common.processing.*;
import org.fiolino.common.reflection.Methods;
import org.fiolino.common.util.Encoder;
import org.fiolino.common.util.Serializer;
import org.fiolino.common.util.Strings;
import org.fiolino.common.util.Types;
import org.fiolino.data.annotation.*;
import org.fiolino.data.base.Identified;
import org.fiolino.data.base.Text;
import org.fiolino.indexer.SolrDocumentFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.lang.invoke.MethodType.methodType;
import static org.fiolino.common.analyzing.Priority.*;

/**
 * Created by kuli on 22.12.15.
 */
class SolrDocumentFactoryBuilder<T> extends Analyzeable {

    private static final Logger logger = LoggerFactory.getLogger(SolrDocumentFactoryBuilder.class);
    private static final int MAX_LENGTH = 0x8000;

    static final Schema SCHEMA = new Schema("Indexer Factory");

    private static final Selector<Prefix> NAME_PREFIX = SCHEMA.createSelector();

    private static final Selector<Filtered> FILTERED = SCHEMA.createSelector(Filtered.NO);

    private static final Selector<String> FACET_NAME = SCHEMA.createSelector();

    private static final Selector<NamingPolicy> NAMING_POLICY = SCHEMA.createLazilyInitializedSelector(
            () -> Beans.get(NamingPolicy.DEFAULT_NAME, NamingPolicy.class));

    private final Prefix prefix;

    private final float boost;

    private final boolean hidden;

    private final boolean isInitial;

    private final Set<String> processedCategories;

    private final Cardinality cardinality;

    private SolrDocumentFiller<T> transporter = SolrDocumentFiller.doNothing();

    private SolrDocumentFactoryBuilder(Prefix prefix, float boost, boolean hidden, Set<String> processedCategories,
                                       Cardinality cardinality) {
        this(prefix, boost, hidden, processedCategories, false, cardinality);
    }

    private SolrDocumentFactoryBuilder(Prefix prefix, float boost, boolean hidden, Set<String> processedCategories,
                                       boolean isInitial, Cardinality cardinality) {
        this.prefix = prefix;
        this.boost = boost;
        this.hidden = hidden;
        this.processedCategories = processedCategories;
        this.isInitial = isInitial;
        this.cardinality = cardinality;
    }

    SolrDocumentFactoryBuilder() {
        this(Prefix.EMPTY, 1.0f, false, new HashSet<>(), true, Cardinality.TO_ONE);
    }

    @Override
    protected MethodHandles.Lookup getLookup() {
        return MethodHandles.lookup();
    }

    @AnnotationInterest(value = INITIALIZING, annotation = Filterable.class)
    private void setFiltered(Container configuration) {
        configuration.set(FILTERED, Filtered.YES);
    }

    private static final Set<Type> FILTERED_TYPES = EnumSet.of(Type.ID, Type.REFERENCE_ID, Type.NAME);

    @AnnotationInterest(INITIALIZING)
    @SuppressWarnings("unused")
    private void registerField(Container configuration, Register annotation) {
        for (Type t : annotation.value()) {
            if (FILTERED_TYPES.contains(t)) {
                setFiltered(configuration);
            }
        }
    }

    @AnnotationInterest(value = INITIALIZING, annotation = Name.class)
    @SuppressWarnings("unused")
    private void setName(Container configuration) {
        setFiltered(configuration);
    }

    @AnnotationInterest(value = INITIALIZING, annotation = Sorts.class)
    @SuppressWarnings("unused")
    private void setFilteredBecauseItsSortable(Container configuration) {
        setFiltered(configuration);
    }

    @AnnotationInterest(INITIALIZING)
    @SuppressWarnings("unused")
    private void setNamingPolicy(Naming naming, Container configuration) {
        NamingPolicy policy = Beans.get(naming.value(), NamingPolicy.class);
        configuration.set(NAMING_POLICY, policy);
    }

    /**
     * Returns only those categories which are not processed yet.
     * The goal is to have each facet category only once.
     */
    private String[] unusedCategories(String[] categories) {
        if (categories.length == 0) {
            return null;
        }
        List<String> duplicates = null;
        for (String c : categories) {
            if (processedCategories.add(c)) {
                continue;
            }
            if (duplicates == null) {
                duplicates = new ArrayList<>(categories.length);
            }
            duplicates.add(c);
        }
        if (duplicates == null) {
            return categories;
        }
        int n = categories.length - duplicates.size();
        if (n == 0) {
            return null;
        }
        String[] remaining = new String[n];
        int x = 0;
        for (String c : categories) {
            if (!duplicates.contains(c)) {
                remaining[x++] = c;
            }
        }
        return remaining;
    }

    @AnnotationInterest(PREPROCESSING)
    @SuppressWarnings("unused")
    private void setDateFacet(ValueDescription field, Container configuration, DateFacet facet) {
        if (!Date.class.isAssignableFrom(field.getValueType())) {
            throw new AssertionError(field + " is annotated with @" + DateFacet.class.getSimpleName() + " but not of type Date.");
        }
        final String monthFacetName = Strings.normalizeName(facet.month()) + "_facetid";
        final String yearFacetName = Strings.normalizeName(facet.year()) + "_facetid";

        MethodHandle getter = field.createGetter();
        if (getter == null) {
            return;
        }
        final MethodHandle myGetter = getter.asType(methodType(Date.class, Object.class));
        addFiller((bean, doc, count) -> {
            Date date;
            try {
                date = (Date) myGetter.invokeExact(bean);
            } catch (Error | RuntimeException e) {
                throw e;
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
            if (date != null) {
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH);
                doc.addField(yearFacetName, year);
                doc.addField(monthFacetName, year * 12 + month);
            }
        });
    }

    @AnnotationInterest(PREPROCESSING)
    @SuppressWarnings("unused")
    private void setFacet(ValueDescription field, Container configuration, Facet facet)
            throws ModelInconsistencyException {
        String[] categories;
        if (facet.allowMulti()) {
            categories = facet.value();
        } else {
            categories = unusedCategories(facet.value());
            if (categories == null) {
                return;
            }
        }
        String name = facet.fieldName();
        if ("".equals(name)) {
            name = Strings.normalizeName(Strings.combinationOf(categories));
        }
        if (Map.class.isAssignableFrom(field.getValueType()) && name.indexOf('*') < 0) {
            name += "_*";
        }
        FieldType fieldType = FieldType.getDefaultFor(field.getTargetType());
        if (fieldType == null) {
            // It's a relation
            indexFacetRelation(field, name);
            return;
        }

        setFiltered(configuration);
        Cardinality fieldCardinality = cardinality.join(field.getGenericType());
        String solrField = name + "_" + fieldType.getSuffix(fieldCardinality, true, false);
        configuration.set(FACET_NAME, solrField);
    }

    private void indexFacetRelation(ValueDescription field, String fieldName) throws ModelInconsistencyException {
        MethodHandle getter = field.createGetter();
        if (getter == null) {
            return;
        }
        final String facetFieldName = fieldName + "_facet";
        final String idFieldName = fieldName + "_facetid";
        field.getConfiguration().set(FACET_NAME, facetFieldName);

        @SuppressWarnings("unchecked")
        final Serializer<Identified> serializer = (Serializer<Identified>) Serializer.get(field.getTargetType());

        if (Iterable.class.isAssignableFrom(getter.type().returnType())) {
            final MethodHandle myGetter = getter.asType(methodType(Iterable.class, Object.class));
            addFiller((bean, doc, count) -> {
                try {
                    @SuppressWarnings("unchecked")
                    Iterable<? extends Identified> relationTarget = (Iterable<? extends Identified>) myGetter.invokeExact(bean);
                    if (relationTarget != null) {
                        for (Identified v : relationTarget) {
                            if (relationShallBeHidden(v)) {
                                continue;
                            }
                            String serialization = serializer.serialize(v);
                            doc.addField(facetFieldName, serialization);
                            doc.addField(idFieldName, v.getId());
                        }
                    }
                } catch (Error | RuntimeException e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError(t);
                }
            });
        } else if (Map.class.isAssignableFrom(getter.type().returnType())) {
            final MethodHandle myGetter = getter.asType(methodType(Map.class, Object.class));
            addFiller((bean, doc, count) -> {
                Map<String, ?> relationTarget;
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> t = (Map<String, ?>) myGetter.invokeExact(bean);
                    relationTarget = t;
                } catch (Error | RuntimeException e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError(t);
                }
                if (relationTarget != null) {
                    for (Map.Entry<String, ?> e : relationTarget.entrySet()) {
                        String key = Encoder.ALL_LETTERS.encode(e.getKey());
                        String solrName = facetFieldName.replace("*", key);
                        String idName = idFieldName.replace("*", key);

                        Object value = e.getValue();
                        if (value == null) {
                            continue;
                        }
                        if (value instanceof Iterable) {
                            ((Iterable<?>) value).forEach(o -> indexEntity(doc, serializer, o, solrName, idName));
                        } else {
                            indexEntity(doc, serializer, value, solrName, idName);
                        }
                    }
                }
            });
        } else {
            final MethodHandle myGetter = getter.asType(methodType(Identified.class, Object.class));
            addFiller((bean, doc, count) -> {
                Identified relationTarget;
                try {
                    relationTarget = (Identified) myGetter.invokeExact(bean);
                } catch (Error | RuntimeException e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError(t);
                }
                if (relationTarget != null && !relationShallBeHidden(relationTarget)) {
                    String serialization = serializer.serialize(relationTarget);
                    doc.addField(facetFieldName, serialization);
                    doc.addField(idFieldName, relationTarget.getId());
                }
            });
        }
    }

    private void indexEntity(SolrInputDocument doc, Serializer<Identified> serializer,
                             Object value, String solrName, String idName) {
        if (relationShallBeHidden(value)) {
            return;
        }
        Identified v = (Identified) value;
        String serialization = serializer.serialize(v);
        doc.addField(solrName, serialization);
        doc.addField(idName, v.getId());
    }

    /**
     * Indexes a to-many relation via serialized data.
     *
     * @param target    The target type
     * @param getter    The getter, must be of type (Object)Iterable
     * @param fieldName the solr field name without suffix
     */
    private void indexToManyRelation(ModelDescription target, Function<T, Collection<?>> getter, String fieldName)
            throws ModelInconsistencyException {
        final String solrName = fieldName + "_rel";

        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) Serializer.get(target.getModelType());

        addFiller((bean, doc, count) -> {
            try {
                Collection<?> relationTarget = getter.apply(bean);
                if (relationTarget != null) {
                    relationTarget.stream().filter(this::relationShallBeHidden).map(serializer::serialize).forEach(
                            s -> doc.addField(solrName, s));
                }
            } catch (Error | RuntimeException e) {
                throw e;
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        });
    }

    private boolean relationShallBeHidden(Object target) {
        return false;
    }

    @AnnotationInterest(INITIALIZING)
    @SuppressWarnings("unused")
    private void setBoostingValue(ValueDescription field, BoostingValue boostingValue) throws ModelInconsistencyException {
        MethodHandle getter = field.createGetter();
        if (getter == null) {
            logger.warn("No getter found for " + field);
            return;
        }
        if (isUnsearched()) {
            return;
        }
        float docBoost = boost * boostingValue.value();
        String prop = boostingValue.property();
        if (!"".equals(prop)) {
            docBoost *= getFloatFromProperty(prop);
        }
        final float myBoost = docBoost;
        Class<?> valueType = field.getValueType();
        if (valueType.equals(String.class)) {
            final MethodHandle getterFromObject = getter.asType(methodType(String.class, Object.class));
            addFiller((source, doc, count) -> {
                String value;
                try {
                    value = (String) getterFromObject.invokeExact(source);
                } catch (Error | RuntimeException e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError(t);
                }
                if (value == null) {
                    return;
                }
                float boost = getFloatFromProperty(value);
                boost *= myBoost;
                boost *= doc.getDocumentBoost();
                doc.setDocumentBoost(boost);
            });
            return;
        }

        Class<?> wrapper = Types.toWrapper(valueType);
        if (wrapper != Boolean.class && !Number.class.isAssignableFrom(wrapper)) {
            throw new ModelInconsistencyException(field + " is neither a String nor a Number.");
        }

        final MethodHandle getterFromObject = MethodHandles.explicitCastArguments(getter, methodType(float.class, Object.class));
        addFiller((source, doc, count) -> {
            float boost;
            try {
                boost = (float) getterFromObject.invokeExact(source);
            } catch (Error | RuntimeException e) {
                throw e;
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
            if (boost <= 0.01f) {
                return;
            }
            boost *= myBoost;
            boost *= doc.getDocumentBoost();
            doc.setDocumentBoost(boost);
        });
    }

    private float getFloatFromProperty(String value) {
        String prop = Beans.getProperty(value);
        return Float.parseFloat(prop);
    }

    @AnnotationInterest(PROCESSING)
    @SuppressWarnings("unused")
    private void setIndexed(Analyzer analyzer, ValueDescription field, Container configuration, Indexed anno)
            throws ModelInconsistencyException {

        MethodHandle getter = field.createGetter();
        if (getter == null) {
            logger.warn("No getter found for " + field);
            return;
        }
        String name = anno.value();
        if ("".equals(name)) {
            name = field.getName();
        }
        Class<?> targetType = field.getTargetType();
        FieldType fieldType = FieldType.getDefaultFor(targetType);
        if (fieldType == null) {
            // Then it's probably a relation
            indexRelation(analyzer, field, name, getter, anno);
            return;
        }
        String fieldName;
        String facetName = configuration.get(FACET_NAME);
        if (facetName == null) {
            fieldName = getSolrName(field, name, fieldType);
        } else {
            fieldName = facetName;
        }
        if (fieldName == null) {
            return;
        }

        SolrDocumentFiller<T> transporter;

        UnaryOperator<Object> converter = findConverter(targetType);
        if (Map.class.isAssignableFrom(field.getValueType())) {
            transporter = createMapTransporter(fieldName, getter, converter, anno);
        } else {
            if (Collection.class.isAssignableFrom(field.getValueType())) {
                transporter = createMultiTransporter(fieldName, getter, converter, anno);
            } else {
                transporter = createTransporter(fieldName, getter, converter, anno);
            }
        }
        addFiller(transporter);
    }

    private UnaryOperator<Object> findConverter(Class<?> type) {
        if (type.isEnum()) {
            return Object::toString;
        }
        return UnaryOperator.identity();
    }

    private boolean isUnsearched() {
        return Float.isNaN(boost) || boost <= 0.0;
    }

    private String getSolrName(ValueDescription field, String name, FieldType fieldType) {
        boolean isHidden = hidden;
        if (fieldType == FieldType.TEXT) {
            // mark hidden text fields as not hidden to have
            // highlighting everywhere
            isHidden = false;
            if (isUnsearched()) {
                fieldType = FieldType.STRING;
            }
        }
        Container configuration = field.getConfiguration();
        Prefix fieldPrefix = configuration.getOrDefault(NAME_PREFIX, prefix);
        NamingPolicy policy = NAMING_POLICY.get(configuration);
        Filtered filtered = FILTERED.get(configuration);
        Cardinality fieldCardinality = cardinality.join(field.getGenericType());

        String[] names;
        if (Map.class.isAssignableFrom(field.getValueType())) {
            String nameWithPlaceholder = name.indexOf('*') < 0 ? name + "_*" : name;
            names = policy.names(new String[] {nameWithPlaceholder}, fieldPrefix, field, fieldType, filtered, fieldCardinality, isHidden);
        } else {
            names = policy.names(new String[] {name}, fieldPrefix, field, fieldType, filtered, fieldCardinality, isHidden);
        }
        if (names == null) {
            return null;
        }

        return names[0];
    }

    private void indexRelation(Analyzer analyzer, ValueDescription field, String name, MethodHandle getter,
                                   Indexed annotation) throws ModelInconsistencyException {
        ModelDescription target = field.getRelationTarget();
        Prefix subPrefix = field.getConfiguration().get(NAME_PREFIX);
        if (subPrefix == null) {
            subPrefix = prefix.newSubPrefix(name);
            field.getConfiguration().set(NAME_PREFIX, subPrefix);
        }
        boolean isToMany = Collection.class.isAssignableFrom(field.getValueType());
        boolean isGeneric = Map.class.isAssignableFrom(field.getValueType());

        float fieldBoost = getFieldBoost(annotation);
        boolean relationIsHidden = hidden || isToMany || isGeneric;
        SolrDocumentFactoryBuilder<Object> subBuilder = new SolrDocumentFactoryBuilder<>(subPrefix,
                boost * fieldBoost, relationIsHidden, processedCategories, cardinality.join(field.getGenericType()));
        analyzer.analyze(target, subBuilder);
        SolrDocumentFiller<Object> subFiller = subBuilder.getFiller();

        if (isToMany) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            Function<T, Collection<?>> getterFunction = (Function<T, Collection<?>>) Methods.lambdafy(getter, Function.class);
            addFiller((bean, doc, count) -> {
                @SuppressWarnings("unchecked")
                Collection<?> value = getterFunction.apply(bean);
                if (value != null) {
                    int subCount = count * value.size();
                    value.stream().filter(this::relationShallBeHidden).forEach(v -> subFiller.process(v, doc, subCount));
                }
            });

            if (field.getConfiguration().get(FACET_NAME) == null) {
                // Then it must be indexed additionally as _rel field
                String[] fullNames = prefix.createNames(name);
                indexToManyRelation(target, getterFunction, fullNames[0]);
            }
        } else if (isGeneric) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            Function<T, Map<String, ?>> getterFunction = (Function<T, Map<String, ?>>) Methods.lambdafy(getter, Function.class);
            addFiller((bean, doc, count) -> {
                Map<String, ?> map = getterFunction.apply(bean);
                if (map != null) {
                    for (Object value : map.values()) {
                        if (value instanceof Collection) {
                            @SuppressWarnings("unchecked")
                            Collection<?> list = (Collection<?>) value;
                            int subCount = count * list.size();
                            list.stream().filter(this::relationShallBeHidden).forEach(v -> subFiller.process(v, doc, subCount));
                        } else {
                            if (relationShallBeHidden(value)) {
                                continue;
                            }
                            subFiller.process(value, doc, count);
                        }
                    }
                }
            });
        } else {
            // To one relation
            @SuppressWarnings({"unchecked", "rawtypes"})
            Function<Object, Object> getterFunction = (Function<Object, Object>) Methods.lambdafy(getter, Function.class);
            addFiller((bean, doc, count) -> {
                @SuppressWarnings("unchecked")
                Object value = getterFunction.apply(bean);
                if (value != null && !relationShallBeHidden(value)) {
                    subFiller.process(value, doc, count);
                }
            });
        }
    }

    private float getFieldBoost(FieldType type, Indexed annotation) {
        if (type != FieldType.TEXT) {
            return 1.0f;
        }
        return getFieldBoost(annotation);
    }

    private float getFieldBoost(Indexed annotation) {
        if (isUnsearched()) {
            return 1.0f;
        }
        if (!isInitial) {
            // Don't apply specific boosting annotations when it's referenced; otherwise names of relations would
            // rank too high
            return boost;
        }
        float fieldBoost = boost * annotation.boost();
        String prop = annotation.boostProperty();
        if (!"".equals(prop)) {
            fieldBoost *= getFloatFromProperty(prop);
        }
        return fieldBoost;
    }

    private static String stripStringValue(String val) {
        if (val == null) {
            return null;
        }
        if (val.length() > MAX_LENGTH) {
            logger.warn("Value too large! Size is " + val.length());
            val = val.substring(0, MAX_LENGTH) + " \u2026";
        }
        return val;
    }

    @SuppressWarnings("unused")
    private static String stripTextValue(Text val) {
        if (val == null) {
            return null;
        }
        return stripStringValue(val.getText());
    }

    private SolrDocumentFiller<T> createTransporter(String solrName, MethodHandle getter, UnaryOperator<Object> converter,
                                                        Indexed annotation) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function<Object, Object> getterFunction = (Function<Object, Object>) Methods.lambdafy(getter, Function.class);
        float b = annotation == null ? boost : getFieldBoost(annotation);
        return (bean, doc, count) -> {
            Object value = getterFunction.apply(bean);
            if (value == null) return;
            if (value instanceof Text) {
                float newBoost = reduceBoostForMultiValueField(b, count);
                doc.addField(solrName, stripTextValue((Text) value), newBoost);
            } else {
                doc.addField(solrName, converter.apply(value));
            }
        };
    }

    private SolrDocumentFiller<T> createMultiTransporter(String solrName, MethodHandle getter, UnaryOperator<Object> converter,
                                                         Indexed annotation) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function<Object, Collection<?>> getterFunction = (Function<Object, Collection<?>>) Methods.lambdafy(getter, Function.class);
        float b = annotation == null ? boost : getFieldBoost(annotation);
        return (bean, doc, count) -> {
            Collection<?> value = getterFunction.apply(bean);
            if (value != null) {
                int fullCount = count * value.size();
                float newBoost = reduceBoostForMultiValueField(b, fullCount);
                for (Object v : value) {
                    if (v instanceof Text) {
                        doc.addField(solrName, stripTextValue((Text) v), newBoost);
                    } else {
                        v = converter.apply(v);
                        doc.addField(solrName, v);
                    }
                }
            }
        };
    }

    private SolrDocumentFiller<T> createMapTransporter(String solrNameTemplate, MethodHandle getter, UnaryOperator<Object> converter, Indexed annotation) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function<Object, Map<String, ?>> getterFunction = (Function<Object, Map<String, ?>>) Methods.lambdafy(getter, Function.class);
        final float b = getFieldBoost(annotation);
        return (bean, doc, count) -> {
            Map<String, ?> map = getterFunction.apply(bean);
            if (map != null) {
                for (Map.Entry<String, ?> e : map.entrySet()) {
                    String key = Encoder.ALL_LETTERS.encode(e.getKey());
                    String solrName = solrNameTemplate.replace("*", key);
                    Object value = e.getValue();
                    if (value instanceof Collection) {
                        int fullCount = count * ((Collection<?>) value).size();
                        float newBoost = reduceBoostForMultiValueField(b, fullCount);
                        for (Object v : (Iterable<?>) value) {
                            if (v instanceof Text) {
                                doc.addField(solrName, stripTextValue((Text) v), newBoost);
                            } else {
                                doc.addField(solrName, converter.apply(v));
                            }
                        }
                    } else if (value instanceof Text) {
                        float newBoost = reduceBoostForMultiValueField(b, count);
                        doc.addField(solrName, stripTextValue((Text) value), newBoost);
                    } else {
                        doc.addField(solrName, converter.apply(value));
                    }
                }
            }
        };
    }

    private float reduceBoostForMultiValueField(float originalBoost, int count) {
        if (isUnsearched()) {
            return 1.0f;
        }
        if (count == 1) {
            return originalBoost;
        }
        return originalBoost / (float) Math.sqrt((double) count);
    }

    private void addFiller(SolrDocumentFiller<T> transporter) {
        this.transporter = this.transporter.andThen(transporter);
    }

    SolrDocumentFiller<T> getFiller() {
        return transporter;
    }
}

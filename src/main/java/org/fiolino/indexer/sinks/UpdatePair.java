package org.fiolino.indexer.sinks;

import javax.annotation.Nullable;

/**
 * Created by Michael Kuhlmann on 14.04.2016.
 */
public abstract class UpdatePair<T> {
    public static <T> UpdatePair<T> onlyNew(T update) {
        return new NewItem<>(update);
    }

    public static <T> UpdatePair<T> withExisting(T update, T existing) {
        return existing == null ? onlyNew(update) : new UpdateItem<>(update, existing);
    }

    private final T update;

    private UpdatePair(T update) {
        this.update = update;
    }

    public T getUpdate() {
        return update;
    }

    @Override
    public String toString() {
        return update.toString();
    }

    public boolean isOf(Class<?> type) {
        return type.isInstance(update);
    }

    public final <U> UpdatePair<U> as(Class<U> newType) {
        if (!isOf(newType)) {
            throw new IllegalArgumentException("Cannot cast " + this + " to " + newType.getName());
        }
        return asUnchecked(newType);
    }

    protected abstract <U> UpdatePair<U> asUnchecked(Class<U> newType);

    @Nullable
    public abstract T getExisting();

    private static class NewItem<T> extends UpdatePair<T> {
        private NewItem(T update) {
            super(update);
        }

        @Nullable
        @Override
        public T getExisting() {
            return null;
        }

        @Override
        protected <U> UpdatePair<U> asUnchecked(Class<U> newType) {
            return onlyNew(newType.cast(getUpdate()));
        }
    }

    private static class UpdateItem<T> extends UpdatePair<T> {
        private final T existing;

        private UpdateItem(T update, T existing) {
            super(update);
            this.existing = existing;
        }

        @Nullable
        @Override
        public T getExisting() {
            return existing;
        }

        @Override
        public String toString() {
            return super.toString() + " <<" + existing + ">>";
        }

        @Override
        public boolean isOf(Class<?> type) {
            return super.isOf(type) && type.isInstance(existing);
        }

        @Override
        protected <U> UpdatePair<U> asUnchecked(Class<U> newType) {
            return withExisting(newType.cast(getUpdate()), newType.cast(getExisting()));
        }
    }
}

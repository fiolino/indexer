package org.fiolino.indexer;

/**
 * Specifies how external data like document content or analytics data shall be upted in the model.
 * <p>
 * Created by Michael Kuhlmann on 14.04.2016.
 */
public enum UpdateStrategy {
    /**
     * Dont update anything, don't even keep existing data - content will be empty after update.
     */
    None(false),

    /**
     * Keep old data from existig items, but don't query for new items. Best choice if service is unavailable.
     */
    Keep(true),

    /**
     * Keep existing data, but query service for new items.
     */
    Initialize(true),

    /**
     * Force update even for existing data.
     */
    Force(false);

    private final boolean needsExistingData;

    UpdateStrategy(boolean needsExistingData) {
        this.needsExistingData = needsExistingData;
    }

    /**
     * If this is set, existing data needs to be loaded from the current index.
     */
    public boolean needsExistingData() {
        return needsExistingData;
    }
}

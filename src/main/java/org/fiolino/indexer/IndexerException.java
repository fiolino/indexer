package org.fiolino.indexer;

/**
 * Created by kuli on 06.10.16.
 */
public class IndexerException extends Exception {

    private static final long serialVersionUID = -1320942839039683460L;

    public IndexerException() {
    }

    public IndexerException(String message) {
        super(message);
    }

    public IndexerException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexerException(Throwable cause) {
        super(cause);
    }
}

package org.sober.hbase.search.repository.serializer;

public class DecodeException extends RuntimeException {
    public DecodeException(Exception e) {
        super(e);
    }
}

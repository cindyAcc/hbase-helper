package org.sober.hbase.search.repository.serializer;

public interface EncoderFactory {
    /**
     * @param type
     * @param <T>
     * @return
     */
    <T> Encoder<T> create(Class<T> type);
}

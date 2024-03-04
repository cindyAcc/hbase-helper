package org.sober.hbase.search.repository.serializer;

public interface DecoderFactory {
    <T> Decoder<T> create(Class<T> type);
}

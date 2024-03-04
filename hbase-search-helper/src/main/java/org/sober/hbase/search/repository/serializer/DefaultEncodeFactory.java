package org.sober.hbase.search.repository.serializer;

public class DefaultEncodeFactory implements EncoderFactory {
    @Override
    public <T> Encoder<T> create(Class<T> type) {
        return new DefaultEncoder<>(type);
    }
}

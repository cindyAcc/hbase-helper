package org.sober.hbase.search.repository.serializer;

public class DefaultDecoderFactory implements DecoderFactory {

    @Override
    public <T> Decoder<T> create(Class<T> type) {
        return new DefaultDecoder<>(type);
    }
}

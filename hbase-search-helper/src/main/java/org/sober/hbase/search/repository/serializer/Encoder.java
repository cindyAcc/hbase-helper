package org.sober.hbase.search.repository.serializer;

import java.util.Collection;

public interface Encoder<T> {
    Collection<EncoderResult> encode(T t);
}

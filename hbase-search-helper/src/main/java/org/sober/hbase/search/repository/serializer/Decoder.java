package org.sober.hbase.search.repository.serializer;

import org.apache.hadoop.hbase.client.Result;

public interface Decoder<T> {

    T decode(Result result);
}

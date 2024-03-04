package org.sober.hbase.search.repository.serializer;

import com.google.common.base.Preconditions;

public class EncoderResult {

    private final byte[] column;
    private final byte[] val;

    public EncoderResult(byte[] column, byte[] val) {
        Preconditions.checkNotNull(column);
        Preconditions.checkNotNull(val);
        this.column = column;
        this.val = val;
    }

    public byte[] getColumn() {
        return column;
    }

    public byte[] getVal() {
        return val;
    }
}

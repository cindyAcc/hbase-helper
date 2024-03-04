package org.sober.hbase.search.repository.serializer;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.util.function.Function;

public class FieldDescription {
    private final Field field;
    private boolean isRowKey;
    private final byte[] qualifier;
    private final Function<String, ?> paramFunc;

    public FieldDescription(Field field,
                            byte[] qualifier,
                            Function<String, ?> paramFunc) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(qualifier);
        Preconditions.checkNotNull(paramFunc);
        this.paramFunc = paramFunc;
        this.field = field;
        this.qualifier = qualifier;
    }

    public FieldDescription(Field field,
                            Function<String, ?> paramFunc) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(paramFunc);
        this.field = field;
        this.isRowKey = true;
        this.qualifier = null;
        this.paramFunc = paramFunc;
    }

    public boolean isRowKey() {
        return isRowKey;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public Field getField() {
        return field;
    }

    public Function<String, ?> getParamFunc() {
        return paramFunc;
    }
}

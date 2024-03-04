package org.sober.hbase.search.repository.serializer;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.sober.hbase.search.util.ModelUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class DefaultDecoder<T> implements Decoder<T> {

    protected final byte[] family;

    protected final Class<T> type;


    protected final Collection<FieldDescription> fieldDescriptions;

    public DefaultDecoder(Class<T> type) {
        this.type = type;
        byte[] family = ModelUtil.getFamily(type);
        this.family = family == null ? HConstants.EMPTY_BYTE_ARRAY : family;
        List<FieldDescription> fieldDescriptions = new ArrayList<>();
        for (Field f : type.getDeclaredFields()) {
            if (ModelUtil.isRowKey(f)) {
                fieldDescriptions.add(new FieldDescription(f, ModelUtil.getFieldParser(f)));
            }
            byte[] qualifier = ModelUtil.getQualifier(f);
            if (qualifier == null) {
                continue;
            }
            Function<String, ?> fieldParser = ModelUtil.getFieldParser(f);
            fieldDescriptions.add(new FieldDescription(f, qualifier, fieldParser));
        }
        this.fieldDescriptions = ImmutableList.copyOf(fieldDescriptions);
    }

    @Override
    public T decode(Result result) {
        T t;
        try {
            t = this.type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DecodeException(e);
        }
        for (FieldDescription fd : this.fieldDescriptions) {
            Field field = fd.getField();
            byte[] val;
            if (fd.isRowKey()) {
                val = result.getRow();
            } else {
                val = result.getValue(this.family, fd.getQualifier());
            }
            if (val == null) {
                continue;
            }
            Object apply = fd.getParamFunc().apply(Bytes.toString(val));
            if (apply == null) {
                continue;
            }
            synchronized (field) {
                field.setAccessible(true);
                try {
                    field.set(t, apply);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new DecodeException(e);
                } finally {
                    field.setAccessible(false);
                }
            }
        }
        return t;
    }
}

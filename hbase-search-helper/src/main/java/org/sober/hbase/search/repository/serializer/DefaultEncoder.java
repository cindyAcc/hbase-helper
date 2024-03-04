package org.sober.hbase.search.repository.serializer;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.util.Bytes;
import org.sober.hbase.search.util.ModelUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DefaultEncoder<T> implements Encoder<T> {

    protected final Class<T> type;

    protected final Collection<FieldDescription> fieldDescriptions;

    public DefaultEncoder(Class<T> type) {
        this.type = type;
        List<FieldDescription> fieldDescriptions = new ArrayList<>();
        for (Field f : type.getDeclaredFields()) {
            byte[] qualifier = ModelUtil.getQualifier(f);
            if (qualifier == null) {
                continue;
            }
            fieldDescriptions.add(new FieldDescription(f, qualifier, ModelUtil.getFieldParser(f)));
        }
        this.fieldDescriptions = ImmutableList.copyOf(fieldDescriptions);
    }

    @Override
    public Collection<EncoderResult> encode(T t) {
        Collection<EncoderResult> encoderResults = new ArrayList<>();
        for (FieldDescription fd : this.fieldDescriptions) {
            Field field = fd.getField();
            Object o;
            synchronized (field) {
                field.setAccessible(true);
                try {
                    o = field.get(t);
                } catch (IllegalAccessException ignore) {
                    o = null;
                } finally {
                    field.setAccessible(false);
                }
            }
            if (o == null) {
                continue;
            }
            encoderResults.add(new EncoderResult(fd.getQualifier(), Bytes.toBytes(String.valueOf(o))));
        }
        return encoderResults;
    }
}

package org.sober.hbase.search.util;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.sober.hbase.search.annotation.Column;
import org.sober.hbase.search.annotation.RowKey;
import org.sober.hbase.search.annotation.Table;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class ModelUtil {

    static final Map<Class<?>, Function<String, ?>> PARAMETER_TYPE_MAP;
    static final Gson GSON = new Gson();
    static final Type STRING_LIST_TYPE = new TypeToken<List<Object>>() {
    }.getType();

    static {
        PARAMETER_TYPE_MAP = new HashMap<>();
        PARAMETER_TYPE_MAP.put(int.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return Integer.parseInt(s);
        });
        PARAMETER_TYPE_MAP.put(Integer.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return Integer.parseInt(s);
        });
        PARAMETER_TYPE_MAP.put(long.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return Long.parseLong(s);
        });
        PARAMETER_TYPE_MAP.put(Long.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return Long.parseLong(s);
        });
        PARAMETER_TYPE_MAP.put(BigInteger.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return new BigInteger(s);
        });
        PARAMETER_TYPE_MAP.put(BigDecimal.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return new BigDecimal(s);
        });
        PARAMETER_TYPE_MAP.put(String.class, s -> s);
        PARAMETER_TYPE_MAP.put(List.class, s -> {
            if (Strings.isNullOrEmpty(s)) {
                return null;
            }
            return GSON.fromJson(s, STRING_LIST_TYPE);
        });
    }

    public static String getTableName(Class<?> type) {
        Table annotation = AnnotationUtils.findAnnotation(type, Table.class);
        if (annotation == null) {
            throw new NullPointerException();
        }
        return annotation.name();
    }

    public static byte[] getFamily(Class<?> type) {
        Table annotation = AnnotationUtils.findAnnotation(type, Table.class);
        String family;
        return annotation == null ? null : Strings.isNullOrEmpty((family = annotation.family())) ? null : Bytes.toBytes(family);
    }

    public static byte[] getQualifier(Field field) {
        Column annotation = AnnotationUtils.findAnnotation(field, Column.class);
        String qualifier;
        return annotation == null ? null : Strings.isNullOrEmpty(qualifier = annotation.value()) ? Bytes.toBytes(field.getName()) : Bytes.toBytes(qualifier);
    }

    public static boolean isRowKey(Field field) {
        return AnnotationUtils.findAnnotation(field, RowKey.class) != null;
    }

    public static Function<String, ?> getFieldParser(Field field) {
        Class<?> type = field.getType();
        return PARAMETER_TYPE_MAP.get(type);
    }
}

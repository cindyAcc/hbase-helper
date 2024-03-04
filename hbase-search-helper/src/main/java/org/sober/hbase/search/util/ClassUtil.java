package org.sober.hbase.search.util;

import org.apache.commons.lang.IllegalClassException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class ClassUtil {

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getInterfaceGenericType(Class<?> c, Class<?> interfaceType) {
        for (Type type : c.getGenericInterfaces()) {
            if (type instanceof ParameterizedType) {
                Type rawType = ((ParameterizedType) type).getRawType();
                if (!interfaceType.equals(rawType)) {
                    continue;
                }
                return (Class<T>) ((ParameterizedType) type).getActualTypeArguments()[0];
            }
        }
        throw new IllegalClassException("can not find generic type from :" + c.getName());
    }
}

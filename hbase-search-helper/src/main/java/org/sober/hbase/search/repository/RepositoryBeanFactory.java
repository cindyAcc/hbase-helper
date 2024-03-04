package org.sober.hbase.search.repository;

import org.apache.hadoop.hbase.client.Connection;
import org.sober.hbase.search.repository.serializer.DecoderFactory;
import org.sober.hbase.search.repository.serializer.EncoderFactory;
import org.sober.hbase.search.util.ClassUtil;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RepositoryBeanFactory<T extends HbaseRepository<?>> implements FactoryBean<T> {

    private final Class<T> clazz;

    private final HbaseRepository<?> hbaseRepository;

    public RepositoryBeanFactory(Class<T> clazz,
                                 DecoderFactory decoderFactory,
                                 EncoderFactory encoderFactory,
                                 Connection connection) {
        this.clazz = clazz;
        Class<?> type = ClassUtil.getInterfaceGenericType(clazz, HbaseRepository.class);
        this.hbaseRepository = new DefaultHbaseRepository<>(type, decoderFactory, encoderFactory, connection);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getObject() throws Exception {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(this.clazz);
        enhancer.setCallback((MethodInterceptor) (o, method, objects, methodProxy) -> RepositoryBeanFactory.this.invoke(method, objects));
        return (T) enhancer.create();
    }

    protected Object invoke(Method method, Object[] objects) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(this.hbaseRepository, objects);
    }

    @Override
    public Class<?> getObjectType() {
        return this.clazz;
    }
}

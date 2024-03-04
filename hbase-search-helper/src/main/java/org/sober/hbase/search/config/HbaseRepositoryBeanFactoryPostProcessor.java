package org.sober.hbase.search.config;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sober.hbase.search.repository.HbaseRepository;
import org.sober.hbase.search.repository.RepositoryBeanFactory;
import org.sober.hbase.search.repository.serializer.DecoderFactory;
import org.sober.hbase.search.repository.serializer.EncoderFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public class HbaseRepositoryBeanFactoryPostProcessor implements BeanFactoryPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(HbaseRepositoryBeanFactoryPostProcessor.class);

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        Map<String, Object> beans = configurableListableBeanFactory.getBeansWithAnnotation(EnableHbaseRepositoryAutoConfig.class);
        if (CollectionUtils.isEmpty(beans)) {
            return;
        }

        Optional<Object> first = beans.values().stream().findFirst();
        if (!first.isPresent()) {
            return;
        }
        String basePackage = this.getBasePackage(first.get());
        Set<Class<? extends HbaseRepository<?>>> repositoryClasses;
        try {
            repositoryClasses = this.scannerPackage(basePackage);
        } catch (IOException e) {
            throw new BeanCreationException("basePackage: " + basePackage + " scanner error");
        }
        try {
            this.initRepositoryBean(configurableListableBeanFactory);
        } catch (Exception e) {
            throw new BeanCreationException("init repositoryBean exception", e);
        }

        for (Class<? extends HbaseRepository<?>> c : repositoryClasses) {
            try {
                String beanName = getBeanName(c);
                if (configurableListableBeanFactory.containsBean(beanName)) {
                    log.warn("hit duplicated bean: {}, ignore...", beanName);
                    continue;
                }
                RepositoryBeanFactory<?> repositoryBeanFactory = this.getRepositoryBeanFactory(configurableListableBeanFactory, c);
                configurableListableBeanFactory.registerSingleton(beanName, Objects.requireNonNull(repositoryBeanFactory.getObject()));
            } catch (Exception exception) {
                throw new BeanCreationException("beanFactory create bean exception", exception);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initRepositoryBean(ConfigurableListableBeanFactory configurableListableBeanFactory) throws Exception {
        Map<String, RepositoryBeanFactory> beansOfType = configurableListableBeanFactory.getBeansOfType(RepositoryBeanFactory.class);
        for (RepositoryBeanFactory<?> beanFactory : beansOfType.values()) {
            String beanName = getBeanName(Objects.requireNonNull(beanFactory.getObjectType()));
            HbaseRepository<?> object = beanFactory.getObject();
            configurableListableBeanFactory.registerSingleton(beanName, Objects.requireNonNull(object));
        }
    }

    protected RepositoryBeanFactory<?> getRepositoryBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory,
                                                                Class<? extends HbaseRepository<?>> c) throws ClassNotFoundException {
        HbaseProperties bean;
        try {
            bean = configurableListableBeanFactory.getBean(HbaseProperties.class);
        } catch (BeansException e) {
            return defaultRepositoryBeanFactory(configurableListableBeanFactory, c);
        }

        String repositoryBeanFactory = bean.getRepositoryBeanFactory();
        if (Strings.isNullOrEmpty(repositoryBeanFactory)) {
            return defaultRepositoryBeanFactory(configurableListableBeanFactory, c);
        }
        Class<?> factoryClass = Class.forName(repositoryBeanFactory);
        Constructor<?>[] constructors = factoryClass.getConstructors();
        for (Constructor<?> constructor : constructors) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            Object[] parameters = Arrays.stream(parameterTypes).map(cl -> {
                if (c.equals(cl)) {
                    return c;
                }
                try {
                    return configurableListableBeanFactory.getBean(cl);
                } catch (Exception ignore) {
                    log.warn("can not find bean type: {}", cl.getName());
                    return null;
                }
            }).toArray(Object[]::new);
            try {
                return (RepositoryBeanFactory<?>) constructor.newInstance(parameters);
            } catch (Exception e) {
                log.warn("can not create by constructor: {}", constructor);
            }
        }
        throw new BeanCreationException("can not create by factory: " + repositoryBeanFactory);
    }

    static RepositoryBeanFactory<?> defaultRepositoryBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory,
                                                                 Class<? extends HbaseRepository<?>> c) {
        String connectionName = getConnectionName(c);
        Connection connection;
        if (Strings.isNullOrEmpty(connectionName)) {
            try {
                connection = configurableListableBeanFactory.getBean(Connection.class);
            } catch (Exception e) {
                log.error("please specify connection name or remove customized connection.");
                throw e;
            }
        } else {
            connection = configurableListableBeanFactory.getBean(connectionName, Connection.class);
        }
        DecoderFactory decoderFactory = configurableListableBeanFactory.getBean(DecoderFactory.class);
        EncoderFactory encoderFactory = configurableListableBeanFactory.getBean(EncoderFactory.class);
        return new RepositoryBeanFactory<>(c, decoderFactory, encoderFactory, connection);
    }

    static String getConnectionName(Class<?> clazz) {
        org.sober.hbase.search.annotation.HbaseRepository annotation = AnnotationUtils.findAnnotation(clazz, org.sober.hbase.search.annotation.HbaseRepository.class);
        String connectionName = "";
        if (annotation != null && !Strings.isNullOrEmpty(annotation.connection())) {
            connectionName = annotation.connection();
        }
        return connectionName;
    }

    static String getBeanName(Class<?> c) {
        org.sober.hbase.search.annotation.HbaseRepository annotation = AnnotationUtils.findAnnotation(c, org.sober.hbase.search.annotation.HbaseRepository.class);
        String name;
        if (annotation != null && !Strings.isNullOrEmpty(name = annotation.value())) {
            return name;
        }
        String s = c.getName();
        if (Character.isLowerCase(s.charAt(0))) {
            return s;
        } else {
            return Character.toLowerCase(s.charAt(0)) + s.substring(1);
        }
    }

    private static final String RESOURCE_PATTERN = "/**/*.class";

    @SuppressWarnings("unchecked")
    private Set<Class<? extends HbaseRepository<?>>> scannerPackage(String basePackage) throws IOException {
        String pattern = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + ClassUtils.convertClassNameToResourcePath(basePackage)
                + RESOURCE_PATTERN;
        PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = pathMatchingResourcePatternResolver.getResources(pattern);
        Set<Class<? extends HbaseRepository<?>>> sets = new HashSet<>();
        MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(pathMatchingResourcePatternResolver);
        for (Resource resource : resources) {
            if (!resource.isReadable()) {
                continue;
            }
            MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
            String className = metadataReader.getClassMetadata().getClassName();
            try {
                Class<?> clazz = Class.forName(className);
                if (!HbaseRepository.class.isAssignableFrom(clazz)) {
                    continue;
                }
                if (AnnotationUtils.findAnnotation(clazz, org.sober.hbase.search.annotation.HbaseRepository.class) == null) {
                    continue;
                }
                sets.add((Class<? extends HbaseRepository<?>>) clazz);
            } catch (ClassNotFoundException ignore) {
            }
        }
        return sets;
    }

    private String getBasePackage(Object o) {
        Class<?> aClass = o.getClass();
        EnableHbaseRepositoryAutoConfig annotation = AnnotationUtils.findAnnotation(aClass, EnableHbaseRepositoryAutoConfig.class);
        String s = Objects.requireNonNull(annotation).basePackage();
        if (Strings.isNullOrEmpty(s)) {
            return aClass.getPackage().getName();
        }
        return s;
    }
}

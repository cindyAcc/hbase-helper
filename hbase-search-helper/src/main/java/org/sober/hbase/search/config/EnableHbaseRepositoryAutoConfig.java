package org.sober.hbase.search.config;

import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Indexed;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Indexed
@Import(EnabledHbaseRepositoryImportSelector.class)
public @interface EnableHbaseRepositoryAutoConfig {

    boolean enabled() default true;

    String basePackage() default "";

    Class<?>[] value() default {HbaseRepositoryAutoConfig.class};
}

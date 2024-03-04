package org.sober.hbase.search.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Repository;

import java.lang.annotation.*;

@Documented
@Repository
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseRepository {

    @AliasFor(annotation = Repository.class)
    String value() default "";

    String connection() default "";
}

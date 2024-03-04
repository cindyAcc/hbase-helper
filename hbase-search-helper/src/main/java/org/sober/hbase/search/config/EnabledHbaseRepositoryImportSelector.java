package org.sober.hbase.search.config;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class EnabledHbaseRepositoryImportSelector implements ImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata metadata) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(
                metadata.getAnnotationAttributes(EnableHbaseRepositoryAutoConfig.class.getName(), true));
        boolean enabled = Objects.requireNonNull(attributes).getBoolean("enabled");
        if (!enabled) {
            return null;
        }
        String[] values = attributes.getStringArray("value");
        if (values.length > 0) {
            List<String> list = Arrays.asList(values);
            return list.toArray(new String[0]);
        }
        return null;
    }
}

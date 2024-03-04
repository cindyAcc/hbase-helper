package org.sober.hbase.search.config;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.sober.hbase.search.repository.serializer.DecoderFactory;
import org.sober.hbase.search.repository.serializer.DefaultDecoderFactory;
import org.sober.hbase.search.repository.serializer.DefaultEncodeFactory;
import org.sober.hbase.search.repository.serializer.EncoderFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.IOException;

@Configuration
@Import(HbaseRepositoryBeanFactoryPostProcessor.class)
public class HbaseRepositoryAutoConfig {

    @Bean
    @ConditionalOnMissingBean
    HbaseProperties hbaseProperties(@Value("${hbase.zookeeper.quorum}") String hosts,
                                    @Value("${hbase.zookeeper.property.clientPort}") String port) {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setHosts(hosts);
        hbaseProperties.setPort(port);
        return hbaseProperties;
    }

    @Bean
    @ConditionalOnMissingBean
    Connection connection(HbaseProperties hbaseProperties) throws IOException {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", hbaseProperties.getHosts());
        configuration.set("hbase.zookeeper.property.clientPort", hbaseProperties.getPort());
        return ConnectionFactory.createConnection(configuration);
    }

    @Bean
    @ConditionalOnMissingBean
    DecoderFactory decoderFactory() {
        return new DefaultDecoderFactory();
    }

    @Bean
    @ConditionalOnMissingBean
    EncoderFactory encoderFactory() {
        return new DefaultEncodeFactory();
    }
}

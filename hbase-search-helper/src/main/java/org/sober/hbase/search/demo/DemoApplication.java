package org.sober.hbase.search.demo;

import org.sober.hbase.search.config.EnableHbaseRepositoryAutoConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableHbaseRepositoryAutoConfig
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

}

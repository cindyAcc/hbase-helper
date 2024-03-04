package org.sober.hbase.search.config;

import org.springframework.beans.factory.annotation.Value;

public class HbaseProperties {
    private String hosts;
    private String port;
    @Value("${hbase.repository.bean-factory:org.sober.hbase.search.repository.RepositoryBeanFactory}")
    private String repositoryBeanFactory;

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getRepositoryBeanFactory() {
        return repositoryBeanFactory;
    }

    public void setRepositoryBeanFactory(String repositoryBeanFactory) {
        this.repositoryBeanFactory = repositoryBeanFactory;
    }
}

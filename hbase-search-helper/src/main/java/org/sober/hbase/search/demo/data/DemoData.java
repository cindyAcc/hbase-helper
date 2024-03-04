package org.sober.hbase.search.demo.data;

import org.sober.hbase.search.annotation.Column;
import org.sober.hbase.search.annotation.Table;

import java.io.Serializable;

@Table(name = "demo", family = "info")
public class DemoData implements Serializable {

    @Column(value = "field_1")
    private String field1;

}

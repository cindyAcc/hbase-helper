package org.sober.hbase.search.demo.repository;

import org.sober.hbase.search.demo.data.DemoData;
import org.sober.hbase.search.repository.HbaseRepository;

@org.sober.hbase.search.annotation.HbaseRepository
public interface DemoRepository extends HbaseRepository<DemoData> {
}

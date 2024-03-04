package org.sober.hbase.search.repository;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface HbaseRepository<T> {

    /**
     * select one row data by key
     *
     * @param key key
     * @return Optional<T>
     */
    Optional<T> selectRow(String key) throws IOException;

    /**
     * select one row data by key
     *
     * @param key key
     * @return data
     * @throws IOException
     */
    default T selectOne(String key) throws IOException {
        return this.selectRow(key).orElse(null);
    }

    /**
     * 查询多个key
     *
     * @param keys keys
     * @return Collection<T>
     * @throws Exception
     */
    Collection<T> selectRows(Collection<String> keys) throws IOException;


    Collection<T> scanTable(Scan scan) throws IOException;

    /**
     * @param startTime
     * @param endTime
     * @return
     * @throws IOException
     */
    Collection<T> scanTable(String startTime, String endTime) throws IOException;

    default Collection<T> scanTable(long start, long end) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(String.valueOf(start)))
                .withStopRow(Bytes.toBytes(String.valueOf(end)));
        return this.scanTable(scan);
    }

    default Collection<T> scanTable() throws IOException {
        Scan scan = new Scan();
        return this.scanTable(scan);
    }

    /**
     * @param rowKeyFilter
     * @return
     * @throws IOException
     */
    default Collection<T> scanTable(String rowKeyFilter) throws IOException {
        if (Strings.isNullOrEmpty(rowKeyFilter)) {
            throw new IllegalArgumentException("param:[rowKeyFilter] is null or empty, please use HbaseRepository#scanTable() instead.");
        }
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyFilter)));
        return this.scanTable(scan);
    }

    /**
     * check table is exists or not
     *
     * @return table is exists
     * @throws IOException
     */
    boolean existsTable() throws IOException;

    /**
     * create table
     *
     * @throws IOException
     */
    void createTable() throws IOException;

    /**
     * delete table
     *
     * @throws IOException
     */
    void deleteTable() throws IOException;

    /**
     * @param key
     * @param t
     * @throws IOException
     */
    void insertOrUpdate(String key, T t) throws IOException;

    /**
     * delete one data
     *
     * @param rowKey
     * @throws IOException
     */
    void delete(String rowKey) throws IOException;

}

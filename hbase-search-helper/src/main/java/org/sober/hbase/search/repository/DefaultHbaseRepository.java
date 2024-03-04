package org.sober.hbase.search.repository;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.sober.hbase.search.repository.serializer.*;
import org.sober.hbase.search.util.ModelUtil;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultHbaseRepository<T> implements HbaseRepository<T> {

    protected final Decoder<T> decoder;

    protected final Encoder<T> encoder;

    protected final TableName tableName;

    protected final Connection connection;

    protected final Collection<FieldDescription> fieldDescriptions;

    protected final byte[] family;

    public DefaultHbaseRepository(Class<T> clazz,
                                  DecoderFactory decoderFactory,
                                  EncoderFactory encoderFactory,
                                  Connection connection) {
        byte[] family = ModelUtil.getFamily(clazz);
        this.family = family == null ? HConstants.EMPTY_BYTE_ARRAY : family;
        List<FieldDescription> fieldDescriptions = new ArrayList<>();
        for (Field f : clazz.getDeclaredFields()) {
            byte[] qualifier = ModelUtil.getQualifier(f);
            if (qualifier == null) {
                continue;
            }
            fieldDescriptions.add(new FieldDescription(f, qualifier, ModelUtil.getFieldParser(f)));
        }
        this.fieldDescriptions = ImmutableList.copyOf(fieldDescriptions);
        this.decoder = decoderFactory.create(clazz);
        this.encoder = encoderFactory.create(clazz);
        this.tableName = TableName.valueOf(ModelUtil.getTableName(clazz));
        this.connection = connection;
    }

    @Override
    public Optional<T> selectRow(String rowKey) throws IOException {
        try (Table table = this.connection.getTable(this.tableName)) {
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            if (result == null || result.isEmpty()) {
                return Optional.empty();
            }
            T mapper = this.decoder.decode(result);
            return Optional.of(mapper);
        }
    }

    @Override
    public Collection<T> selectRows(Collection<String> rowKeys) throws IOException {
        try (Table table = this.connection.getTable(this.tableName)) {
            List<Get> collect = rowKeys.parallelStream().map(rowKey -> new Get(Bytes.toBytes(rowKey))).collect(Collectors.toList());
            Result[] results = table.get(collect);
            List<T> result = new ArrayList<>();
            for (Result r : results) {
                if (r.isEmpty()) {
                    continue;
                }
                result.add(this.decoder.decode(r));
            }
            return result;
        }
    }

    @Override
    public Collection<T> scanTable(String startTime, String endTime) throws IOException {
        Collection<T> results = new ArrayList<>();
        try (Table table = this.connection.getTable(this.tableName)) {
            for (char i = 'a'; i <= 'f'; i++) {
                Scan scan = new Scan();
                scan.withStartRow(Bytes.toBytes(String.valueOf(i).concat(startTime)))
                        .withStopRow(Bytes.toBytes(String.valueOf(i).concat(endTime)));
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result next : scanner) {
                        if (next.isEmpty()) {
                            continue;
                        }
                        T decode = this.decoder.decode(next);
                        results.add(decode);
                    }
                }
            }
        }
        return results;
    }

    @Override
    public Collection<T> scanTable(Scan scan) throws IOException {
        Collection<T> results = new ArrayList<>();
        try (Table table = this.connection.getTable(this.tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result next : scanner) {
                if (next.isEmpty()) {
                    continue;
                }
                T decode = this.decoder.decode(next);
                results.add(decode);
            }
        }
        return results;
    }

    @Override
    public boolean existsTable() throws IOException {
        try (Admin admin = this.connection.getAdmin()) {
            return admin.tableExists(this.tableName);
        }
    }

    @Override
    public void createTable() throws IOException {
        if (this.existsTable()) {
            throw new TableExistsException();
        }
        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(this.tableName);
        List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
        for (FieldDescription description : this.fieldDescriptions) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(description.getQualifier()).build();
            columnFamilyList.add(columnFamilyDescriptor);
        }
        descriptorBuilder.setColumnFamilies(columnFamilyList);
        TableDescriptor tableDescriptor = descriptorBuilder.build();
        try (Admin admin = this.connection.getAdmin()) {
            admin.createTable(tableDescriptor);
        }
    }

    @Override
    public void deleteTable() throws IOException {
        try (Admin admin = this.connection.getAdmin()) {
            if (!admin.tableExists(this.tableName)) {
                return;
            }
            admin.disableTable(this.tableName);
            admin.deleteTable(this.tableName);
        }
    }

    @Override
    public void insertOrUpdate(String key, T t) throws IOException {
        Put put = new Put(Bytes.toBytes(key));
        try (Table table = this.connection.getTable(this.tableName)) {
            Collection<EncoderResult> encode = this.encoder.encode(t);
            if (CollectionUtils.isEmpty(encode)) {
                throw new IllegalArgumentException("empty data");
            }
            for (EncoderResult er : encode) {
                put.addColumn(this.family, er.getColumn(), er.getVal());
            }
            table.put(put);
        }
    }

    @Override
    public void delete(String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try (Table table = this.connection.getTable(this.tableName)) {
            table.delete(delete);
        }
    }
}

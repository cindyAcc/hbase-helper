package org.sober.filter.demo;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DemoFilter extends FilterBase {

    /**
     * offset limit 相当于 sql 语句中的 `LIMIT offset, size` 中的 `offset` 和 `size`
     * 用于分页
     */
    private int limit = 0;
    private int offset = 0;
    // count 用于计数 已获取的记录条数
    private int count = 0;

    // 过滤的字段
    private String filterField = null;
    // 过滤字段值
    private String filterValue = null;


    public DemoFilter(int limit, int offset, int count, String filterField, String filterValue) {
        this.limit = limit;
        this.offset = offset;
        this.count = count;
        this.filterField = filterField;
        this.filterValue = filterValue;
    }

    @Override
    public ReturnCode filterKeyValue(Cell c) throws IOException {
        // 过滤逻辑
        if (this.count >= this.offset + this.limit) {
            // 如果已经获取的记录条数大于等于 offset + limit,则返回 下一行
            return ReturnCode.NEXT_ROW;
        }
        ReturnCode ret = ReturnCode.NEXT_COL;
        String columnValue = Bytes.toString(CellUtil.cloneValue(c));
        JSONObject jsonObject = JSONObject.parseObject(columnValue);
        String value = jsonObject.getString(filterField);
        if (value != null && value.equals(filterValue)) {
            if (this.count >= this.offset && this.count < offset + limit) {
                ret = ReturnCode.INCLUDE_AND_NEXT_COL;
            }
            this.count++;
        }
        return ret;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        DemoFilterProtoV1.DemoFilterProto.Builder builder = DemoFilterProtoV1.DemoFilterProto.newBuilder();
        builder.setLimit(this.limit);
        builder.setOffset(this.offset);
        builder.setCount(this.count);
        builder.setFilterField(this.filterField);
        builder.setFilterValue(this.filterValue);
        return builder.build().toByteArray();
    }

    public static DemoFilter parseFrom(byte[] data) throws DeserializationException {
        try {
            DemoFilterProtoV1.DemoFilterProto proto = DemoFilterProtoV1.DemoFilterProto.parseFrom(data);
            return new DemoFilter(proto.getLimit(), proto.getOffset(), proto.getCount(), proto.getFilterField(), proto.getFilterValue());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
    }

}

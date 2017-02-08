package com.alicloud.tablestore.hbase;

import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.filter.CompareFilter.*;

public class ColumnMapping {

    private static final String GLOBAL_FAMILY_CONF_KEY = "hbase.client.tablestore.family";
    private static final String DEFAULT_FAMILY_NAME = "f";

    private byte[] familyNameBytes;

    public ColumnMapping(String tableName, Configuration conf) {
        String tableFamilyName = conf.get(GLOBAL_FAMILY_CONF_KEY + "." + tableName,
                conf.get(GLOBAL_FAMILY_CONF_KEY, DEFAULT_FAMILY_NAME));
        this.familyNameBytes = Bytes.toBytes(tableFamilyName);
    }

    public boolean isSupportMultiColumnFamily() {
        return false;
    }

    public byte[] getTablestoreColumn(byte[] columnFamily, byte[] column) {
        return column;
    }

    public SingleColumnValueCondition.CompareOperator getTablestoreCompareOp(CompareOp op) {
        switch (op) {
            case EQUAL:
                return SingleColumnValueCondition.CompareOperator.EQUAL;
            case NOT_EQUAL:
                return SingleColumnValueCondition.CompareOperator.NOT_EQUAL;
            case GREATER:
                return SingleColumnValueCondition.CompareOperator.GREATER_THAN;
            case GREATER_OR_EQUAL:
                return SingleColumnValueCondition.CompareOperator.GREATER_EQUAL;
            case LESS:
                return SingleColumnValueCondition.CompareOperator.LESS_THAN;
            case LESS_OR_EQUAL:
                return SingleColumnValueCondition.CompareOperator.LESS_EQUAL;
            default:
                throw new UnsupportedOperationException(op.name());
        }
    }

    public byte[][] getHBaseColumn(byte[] otsColumn) {
        return new byte[][] { this.familyNameBytes, otsColumn };
    }

    public byte[] getFamilyNameBytes() {
        return this.familyNameBytes;
    }
}

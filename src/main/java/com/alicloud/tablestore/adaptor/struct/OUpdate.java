package com.alicloud.tablestore.adaptor.struct;


import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;
import org.apache.hadoop.hbase.client.WrongRowIOException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class OUpdate extends OMutation implements Comparable<ORow> {
    public OUpdate(byte[] row) {
        Preconditions.checkNotNull(row);

        this.row = Arrays.copyOf(row, row.length);
    }

    public OUpdate(OUpdate updateToCopy) {
        this(updateToCopy.getRow());
        this.keyValues.addAll(updateToCopy.keyValues);
    }

    public void add(OPut p) throws IOException {
        internalAdd(p);
    }

    public void add(ODelete d) throws IOException {
        internalAdd(d);
    }

    private void internalAdd(OMutation m) throws IOException {
        int res = Bytes.compareTo(this.row, m.getRow());
        if (res != 0) {
            throw new WrongRowIOException("The row in the recently added Put/Delete <" +
                    Bytes.toStringBinary(m.getRow()) + "> doesn't match the original one <" +
                    Bytes.toStringBinary(this.row) + ">");
        }
        this.keyValues.addAll(m.getKeyValues());
    }

    public RowUpdateChange toOTSParameter(String tableName) {
        PrimaryKey primaryKey = OTSUtil.toPrimaryKey(getRow(), OTSConstants.PRIMARY_KEY_NAME);
        RowUpdateChange ruc = new RowUpdateChange(tableName, primaryKey);
        for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
            if (kv.getType() == OColumnValue.Type.PUT) {
                if (kv.getTimestamp() == OTSConstants.LATEST_TIMESTAMP) {
                    ruc.put(Bytes.toString(kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()));
                } else {
                    ruc.put(Bytes.toString(kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()), kv.getTimestamp());
                }
            } else if (kv.getType() == OColumnValue.Type.DELETE) {
                ruc.deleteColumn(Bytes.toString(kv.getQualifier()), kv.getTimestamp());
            } else if (kv.getType() == OColumnValue.Type.DELETE_ALL) {
                ruc.deleteColumns(Bytes.toString(kv.getQualifier()));
            }
        }
        ruc.setCondition(getCondition());
        return ruc;
    }
}

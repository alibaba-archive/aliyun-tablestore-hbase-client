package com.alicloud.tablestore.hbase;

import java.io.IOException;
import java.util.*;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.tablestore.adaptor.filter.OColumnPaginationFilter;
import com.alicloud.tablestore.adaptor.struct.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import com.alicloud.tablestore.adaptor.filter.OFilter;
import com.alicloud.tablestore.adaptor.filter.OFilterList;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter.OCompareOp;

public class ElementConvertor {
    private static final long DEFAULT_MAX_RESULT_SIZE = -1L;

    /**
     * Creates a {@link ODelete} (Tablestore) from a {@link Delete} (HBase).
     *
     * @param in the <code>Delete</code> to convert
     *
     * @return converted <code>ODelete</code>
     */
    public static ODelete toOtsDelete(Delete in, ColumnMapping columnMapping) {
        ODelete out = new ODelete(in.getRow());
        validateMultiFamilySupport(in.getFamilyCellMap().keySet(), columnMapping, true);

        checkDeleteSupport(in);

        for (Map.Entry<byte[], List<Cell>> familyEntry : in.getFamilyCellMap().entrySet()) {
            for (Cell cell : familyEntry.getValue()) {
                if (!CellUtil.isDelete(cell)) {
                    continue;
                }

                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                long timestamp = cell.getTimestamp();
                KeyValue.Type kvType = KeyValue.Type.codeToType(cell.getTypeByte());
                if (kvType.equals(KeyValue.Type.Delete)) {
                    if (timestamp == HConstants.LATEST_TIMESTAMP) {
                        throw new UnsupportedOperationException(
                                "Delete leatest version is not supportted");
                    } else {
                        out.deleteColumn(columnMapping.getTablestoreColumn(family, qualifier), timestamp);
                    }
                } else if (kvType.equals(KeyValue.Type.DeleteColumn)) {
                    if (timestamp == HConstants.LATEST_TIMESTAMP) {
                        out.deleteColumns(columnMapping.getTablestoreColumn(family, qualifier));
                    } else {
                        throw new UnsupportedOperationException(
                                "Delete versions less than specified timestamp is not supportted");
                    }
                } else if (kvType.equals(KeyValue.Type.DeleteFamily)) {
                    // this means delete whole row in OTS
                }
            }
        }
        return out;
    }

    public static List<Delete> toHBaseDeletes(List<ODelete> in, ColumnMapping columnMapping) {
        List<Delete> out = new ArrayList<Delete>(in.size());
        for (ODelete delete : in) {
            out.add(toHBaseDelete(delete, columnMapping));
        }
        return out;
    }

    public static Delete toHBaseDelete(ODelete in, ColumnMapping columnMapping) {
        Delete out = new Delete(in.getRow());

        for (OColumnValue columnValue: in.getKeyValues()) {
            byte[] family = columnMapping.getFamilyNameBytes();
            byte[] qualifier = columnValue.getQualifier();
            long timestamp = columnValue.getTimestamp();

            OColumnValue.Type type = columnValue.getType();
            if (type == OColumnValue.Type.DELETE) {
                out.addColumn(family, qualifier, timestamp);
            } else if (type == OColumnValue.Type.DELETE_ALL) {
                out.addColumns(family, qualifier, timestamp);
            }
        }

        return out;
    }

    private static void checkDeleteSupport(Delete in) throws UnsupportedOperationException{
        if (in.getACL() != null) {
            throw new UnsupportedOperationException("Delete#setACL() is not supported");
        }
        if (in.getTimeStamp() != Long.MAX_VALUE) {
            throw new UnsupportedOperationException("Delete#setTimeStamp() is not supported");
        }
        if (in.getTTL() != Long.MAX_VALUE) {
            throw new UnsupportedOperationException("Delete#setTTL() is not supported");
        }
        if (in.getDurability() != Durability.USE_DEFAULT) {
            throw new UnsupportedOperationException("Delete#setDurability() is not supported");
        }
        if (!in.getAttributesMap().isEmpty()) {
            throw new UnsupportedOperationException("Delete#setAttribute() is not supported");
        }
    }

    private static void validateMultiFamilySupport(Set<byte[]> familySet,
                                                   ColumnMapping columnMapping,
                                                   boolean isAllowEmpty)
    {
        if (familySet.size() == 0 && !isAllowEmpty) {
            throw new IllegalArgumentException("Column family is not set");
        } else if (familySet.size() == 0 && isAllowEmpty) {
            return;
        }

        if(familySet.size() > 1 && !columnMapping.isSupportMultiColumnFamily()) {
            throw new UnsupportedOperationException("multi-columnFamily is not supported");
        }

        byte[] inputFamily = familySet.iterator().next();
        byte[] configFamily = columnMapping.getFamilyNameBytes();
        if (!Bytes.equals(inputFamily, configFamily)) {
            throw new UnsupportedOperationException("multi-columnFamily is not supported, input:" + Bytes.toString(inputFamily)
                    + ",config:" + Bytes.toString(configFamily));
        }
    }

    /**
     * Converts multiple {@link Delete}s (HBase) into a list of {@link ODelete}s
     * (OTS).
     *
     * @param in list of <code>Delete</code>s to convert
     *
     * @return list of converted <code>ODelete</code>s
     *
     * @see #toOtsDelete(Delete, ColumnMapping)
     */
    public static List<ODelete> toOtsDeleteList(List<Delete> in, ColumnMapping columnMapping) {
        List<ODelete> out = new ArrayList<ODelete>(in.size());
        for (Delete delete : in) {
            if (delete != null) {
                out.add(toOtsDelete(delete, columnMapping));
            }
        }
        return out;
    }

    /**
     * Creates a {@link OGet} (OTS) from a {@link Get} (HBase).
     *
     *
     * @param in the <code>Get</code> to convert
     *
     * @return <code>OGet</code> object
     * @throws IOException
     */
    static OGet toOtsGet(Get in, ColumnMapping columnMapping) throws IOException,UnsupportedOperationException {
        OGet out = new OGet(in.getRow());
        validateMultiFamilySupport(in.getFamilyMap().keySet(), columnMapping, true);
        if (!in.getFamilyMap().isEmpty()) {
            Map<byte[], NavigableSet<byte[]>> familyMap = in.getFamilyMap();
            for (Map.Entry<byte[], NavigableSet<byte[]>> familyEntry : familyMap
                    .entrySet()) {
                byte[] family = familyEntry.getKey();
                if (familyEntry.getValue() == null || familyEntry.getValue().isEmpty()) {
                    // this means get whole row in OTS
                } else {
                    for (byte[] qualifier : familyEntry.getValue()) {
                        out.addColumn(columnMapping.getTablestoreColumn(family, qualifier));
                    }
                }
            }
        }
        if (in.getMaxVersions() > 1) {
            out.setMaxVersions(in.getMaxVersions());
        }
        if (!in.getTimeRange().isAllTime()) {
            TimeRange tr = in.getTimeRange();
            out.setTimeRange(tr.getMin(), tr.getMax());
        }
        if (in.getFilter() != null) {
            out.setFilter(toOtsFilter(in.getFilter(),columnMapping));
        }

        checkGetRowSupport(in);

        return out;
    }

    private static void checkGetRowSupport(Get in) {
        // unsupport method
        if (in.getColumnFamilyTimeRange().size() > 0) {
            throw new UnsupportedOperationException("Get#setColumnFamilyTimeRange() is not supported");
        }
        if (in.isClosestRowBefore()) {
            throw new UnsupportedOperationException("Get#setClosestRowBefore(true) is not supported");
        }
        if (!in.getCacheBlocks()) {
            throw new UnsupportedOperationException("Get#setCacheBlocks(false) is not supported");
        }
        if (in.getACL() != null) {
            throw new UnsupportedOperationException("Get#setACL() is not supported");
        }
        if (in.getMaxResultsPerColumnFamily() != -1) {
            throw new UnsupportedOperationException("Get#setMaxResultsPerColumnFamily() is not supported");
        }
        if (in.getRowOffsetPerColumnFamily() != 0) {
            throw new UnsupportedOperationException("Get#setRowOffsetPerColumnFamily() is not supported");
        }
        if (in.getConsistency() == Consistency.TIMELINE) {
            throw new UnsupportedOperationException("Get#setConsistency(Consistency.TIMELINE) is not supported");
        }
        if (in.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
            throw new UnsupportedOperationException("Get#setIsolationLevel(READ_UNCOMMITTED) is not supported");
        }
        if (in.getReplicaId() > 0) {
            throw new UnsupportedOperationException("Get#setReplicaId() is not supported");
        }
    }

    private static OFilter toOtsFilter(Filter filter, ColumnMapping columnMapping) {
        if (filter instanceof FilterList) {
            List<OFilter> otsFilters = new ArrayList<OFilter>();
            for (Filter hbaseFilter : ((FilterList) filter).getFilters()) {
                otsFilters.add(toOtsFilter(hbaseFilter,columnMapping));
            }
            OFilterList.Operator operator = OFilterList.Operator
                    .valueOf(((FilterList) filter).getOperator().name());
            return new OFilterList(operator, otsFilters);
        } else {
            if (filter instanceof SingleColumnValueFilter) {
                return toOtsSingleColumnValueFilter((SingleColumnValueFilter) filter, columnMapping);
            } else if (filter instanceof ColumnPaginationFilter) {
                return toOtsColumnPaginationFilter((ColumnPaginationFilter)filter);
            } else {
                throw new IllegalArgumentException("unsupported filter type "
                        + filter.getClass().getName()
                        + " only SingleColumnValueFilter is allowed.");
            }
        }
    }

    private static OFilter toOtsSingleColumnValueFilter(
            SingleColumnValueFilter hbaseFilter,
            ColumnMapping columnMapping) {
        if ((hbaseFilter.getComparator() instanceof BinaryComparator)) {
            OCompareOp compareOp = OCompareOp.valueOf(hbaseFilter.getOperator()
                    .name());
            byte[] otsColumn = columnMapping.getTablestoreColumn(hbaseFilter.getFamily(),
                    hbaseFilter.getQualifier());
            OSingleColumnValueFilter otsFilter = new OSingleColumnValueFilter(
                    otsColumn, compareOp, hbaseFilter.getComparator().getValue());
            otsFilter.setFilterIfMissing(hbaseFilter.getFilterIfMissing());
            otsFilter.setLatestVersionOnly(hbaseFilter.getLatestVersionOnly());
            return otsFilter;
        } else {
            throw new UnsupportedOperationException(hbaseFilter.getComparator().getClass().getName());
        }
    }

    private static OFilter toOtsColumnPaginationFilter(ColumnPaginationFilter filter) {
        if (filter.getColumnOffset() != null) {
            return new OColumnPaginationFilter(filter.getLimit(), filter.getColumnOffset());
        } else {
            return new OColumnPaginationFilter(filter.getLimit(), filter.getOffset());
        }
    }

    /**
     * Converts multiple {@link OPut}s (OTS) into a list of {@link Put}s (HBase).
     *
     * @param in list of <code>OPut</code>s to convert
     *
     * @return list of converted <code>Put</code>s
     */
    public static List<Put> toHBasePuts(List<OPut> in, ColumnMapping columnMapping) {
        List<Put> out = new ArrayList<Put>(in.size());
        for (OPut put : in) {
            out.add(toHBasePut(put, columnMapping));
        }
        return out;
    }

    /**
     * Creates a {@link Put} (HBase) from a {@link OPut} (OTS)
     *
     * @param in the <code>OPut</code> to convert
     *
     * @return converted <code>Put</code>
     */
    public static Put toHBasePut(OPut in, ColumnMapping columnMapping) {
        if (in.isEmpty()) {
            throw new IllegalArgumentException("No columns to insert");
        }
        Put out = new Put(in.getRow());
        for (OColumnValue kv : in.getKeyValueList()) {
            byte[][] hbasecolumn = columnMapping.getHBaseColumn(kv.getQualifier());
            out.addColumn(hbasecolumn[0], hbasecolumn[1], kv.getTimestamp(), kv.getValue());
        }
        return out;
    }

    /**
     * Converts multiple {@link Put}s (HBase) into a list of {@link OPut}s (OTS).
     *
     * @param in list of <code>Put</code>s to convert
     *
     * @return list of converted <code>OPut</code>s
     *
     * @see #toOtsPut(Put, ColumnMapping)
     */
    public static List<OPut> toOtsPuts(List<Put> in, ColumnMapping columnMapping) {
        List<OPut> out = new ArrayList<OPut>(in.size());
        for (Put put : in) {
            out.add(toOtsPut(put, columnMapping));
        }
        return out;
    }

    /**
     * Creates a {@link OPut} (OTS) from a {@link Put} (HBase)
     *
     * @param in the <code>Put</code> to convert
     *
     * @return converted <code>OPut</code>
     */
    public static OPut toOtsPut(Put in, ColumnMapping columnMapping) {
        if (in.isEmpty()) {
            throw new IllegalArgumentException("No columns to insert");
        }
        validateMultiFamilySupport(in.getFamilyCellMap().keySet(),columnMapping, false);
        OPut out = new OPut(in.getRow());
        for (List<Cell> list : in.getFamilyCellMap().values()) {
            for (Cell cell : list) {
                out.add(columnMapping.getTablestoreColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)),
                        cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
        return out;
    }

    public static OUpdate toOtsUpdate(RowMutations in, ColumnMapping columnMapping) throws IOException {
        OUpdate out = new OUpdate(in.getRow());
        for (Mutation mutation : in.getMutations()) {
            if (mutation instanceof Put) {
                OPut put = toOtsPut((Put)mutation, columnMapping);
                out.add(put);
            } else if (mutation instanceof  Delete) {
                ODelete delete = toOtsDelete((Delete)mutation, columnMapping);
                out.add(delete);
            }
        }
        return out;
    }

    public static Condition toOtsCondition(byte[] family, byte[] qualifier,
                                           CompareFilter.CompareOp compareOp, byte[] value,
                                           ColumnMapping columnMapping)
    {
        byte[] columnValue = value == null ? new byte[0] : value;
        String columnName = Bytes.toString(columnMapping.getTablestoreColumn(family, qualifier));
        SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(columnName,
                columnMapping.getTablestoreCompareOp(compareOp),
                ColumnValue.fromBinary(columnValue));

        if (columnValue.length == 0) {
            columnCondition.setPassIfMissing(true);
        } else {
            columnCondition.setPassIfMissing(false);
        }
        Condition condition = new Condition();
        condition.setColumnCondition(columnCondition);
        return condition;
    }

    public static OScan toOtsScan(Scan in, ColumnMapping columnMapping) throws IOException {
        OScan out = new OScan();
        validateMultiFamilySupport(in.getFamilyMap().keySet(),columnMapping, true);
        if (!in.getFamilyMap().isEmpty()) {
            Map<byte[], NavigableSet<byte[]>> familyMap = in.getFamilyMap();
            for (Map.Entry<byte[], NavigableSet<byte[]>> familyEntry : familyMap
                    .entrySet()) {
                byte[] family = familyEntry.getKey();
                if (familyEntry.getValue() == null || familyEntry.getValue().isEmpty()) {
                    // this means scan whole rows in OTS
                } else {
                    for (byte[] qualifier : familyEntry.getValue()) {
                        out.addColumn(columnMapping.getTablestoreColumn(family, qualifier));
                    }
                }
            }
        }

        checkScanSupport(in);

        if (!Bytes.equals(in.getStartRow(), HConstants.EMPTY_START_ROW)) {
            out.setStartRow(in.getStartRow());
        }
        if (!Bytes.equals(in.getStopRow(), HConstants.EMPTY_END_ROW)) {
            out.setStopRow(in.getStopRow());
        }
        if (in.getCaching() > 0) {
            out.setCaching(in.getCaching());
        }
        if (in.getMaxVersions() > 1) {
            out.setMaxVersions(in.getMaxVersions());
        }
        if (!in.getTimeRange().isAllTime()) {
            TimeRange tr = in.getTimeRange();
            out.setTimeRange(tr.getMin(), tr.getMax());
        }

        out.setReversed(in.isReversed());

        if (in.getFilter() != null) {
            out.setFilter(toOtsFilter(in.getFilter(), columnMapping));
        }

        return out;
    }

    private static void checkScanSupport(Scan in) {
        if(in.getBatch() != -1) {
            throw new UnsupportedOperationException("Scan#Batch() is not supported");
        }
        if(in.getMaxResultSize() != DEFAULT_MAX_RESULT_SIZE) {
            throw new UnsupportedOperationException("Scan#MaxResultSize is not supported");
        }
        if(in.isRaw()) {
            throw new UnsupportedOperationException("Scan#SetRaw() is not supported");
        }
        if(!in.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED)) {
            throw new UnsupportedOperationException("Scan#setIsolationLevel(READ_UNCOMMITTED) is not support");
        }
        if (in.getReplicaId() != -1) {
            throw new UnsupportedOperationException("Scan#setReplicaId() is not support");
        }
        if (in.getACL() != null) {
            throw new UnsupportedOperationException("Scan#setACL() is not support");
        }
        if (in.getConsistency().equals(Consistency.TIMELINE)) {
            throw new UnsupportedOperationException("Scan#setConsistency(TIMELINE) is not support");
        }
        if (!in.getAttributesMap().isEmpty()) {
            throw new UnsupportedOperationException("Scan#setAttribute() is not support");
        }
        if (in.getAllowPartialResults()) {
            throw new UnsupportedOperationException("Scan#setAllowPartialResults() is not support");
        }
        if (!in.getCacheBlocks()) {
            throw new UnsupportedOperationException("Scan#setCacheBlocks(false) is not support");
        }
        if (in.getLoadColumnFamiliesOnDemandValue() != null) {
            throw new UnsupportedOperationException("Scan#setLoadColumnFamiliesOnDemandValue() is not support");
        }
        if (!in.getColumnFamilyTimeRange().isEmpty()) {
            throw new UnsupportedOperationException("Scan#setColumnFamilyTimeRange() is not support");
        }
        if (in.getRowOffsetPerColumnFamily() > 0) {
            throw new UnsupportedOperationException("Scan#setRowOffsetPerColumnFamily() is not support");
        }
        if (in.getMaxResultsPerColumnFamily() > 0) {
            throw new UnsupportedOperationException("Scan#setMaxResultsPerColumnFamily() is not support");
        }
        if (in.isSmall()) {
            throw new UnsupportedOperationException("Scan#setSmall() is not support");
        }
    }

    /**
     * Converts multiple {@link Get}s (HBase) into a list of {@link OGet}s (OTS).
     *
     * @param in list of <code>Get</code>s to convert
     *
     * @return list of <code>OGet</code> objects
     * @throws IOException
     *
     * @see #toOtsGet(Get, ColumnMapping)
     */
    public static List<OGet> toOtsGets(List<Get> in, ColumnMapping columnMapping) throws IOException {
        List<OGet> out = new ArrayList<OGet>(in.size());
        for (Get get : in) {
            out.add(toOtsGet(get, columnMapping));
        }
        return out;
    }

    /**
     * Converts multiple {@link OResult}s (OTS) into a list of {@link Result}s
     * (HBase).
     *
     * @param in array of <code>OResult</code>s to convert
     *
     * @return array of converted <code>Result</code>s
     *
     * @see #toHBaseResult(OResult, ColumnMapping)
     */
    public static Result[] toHBaseResults(List<OResult> in, ColumnMapping columnMapping) {
        List<Result> out = new ArrayList<Result>(in.size());
        for (OResult result : in) {
            out.add(toHBaseResult(result, columnMapping));
        }
        return out.toArray(new Result[0]);
    }

    /**
     * Create a {@link Result} (HBase) from a {@link OResult} (OTS)
     *
     * @param in
     * @return converted result
     */
    public static Result toHBaseResult(OResult in,
                                       ColumnMapping columnMapping) {
        List<Cell> cells = new ArrayList<Cell>(in.size());
        for (OColumnValue columnValue : in.raw()) {
            byte[][] hbaseColumn = columnMapping.getHBaseColumn(columnValue.getQualifier());
            Cell cell = CellUtil.createCell(in.getRow(), hbaseColumn[0], hbaseColumn[1],
                    columnValue.getTimestamp(), KeyValue.Type.Put.getCode(), columnValue.getValue());
            cells.add(cell);
        }
        return Result.create(cells);
    }

    public static HTableDescriptor toHbaseTableDescriptor(OTableDescriptor in, ColumnMapping columnMapping) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(in.getTableName()));

        HColumnDescriptor family = new HColumnDescriptor(columnMapping.getFamilyNameBytes());
        family.setMaxVersions(in.getMaxVersion());
        family.setTimeToLive(in.getTimeToLive());
        family.setBlockCacheEnabled(true);
        tableDescriptor.addFamily(family);
        return tableDescriptor;
    }
}

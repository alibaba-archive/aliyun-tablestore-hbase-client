package com.alicloud.tablestore.hbase.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestGetRow {
    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_get_row_";

    public TestGetRow() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        familyName = config.get("hbase.client.tablestore.family");

        TableName tableName = TableName.valueOf(config.get("hbase.client.tablestore.table"));
        if (!connection.getAdmin().tableExists(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            connection.getAdmin().createTable(descriptor);
            TimeUnit.SECONDS.sleep(1);
        }
        table = connection.getTable(tableName);
    }

    private void clean() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);

        for (Result row : scanResult) {
            Delete delete = new Delete(row.getRow());
            table.delete(delete);
        }
    }

    @Test
    public void testGetRowWithOneColumn() throws IOException {
        clean();

        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }

    @Test
    public void testGetRowWithTwoColumn() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 1);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName1 = Bytes.toBytes("col_1");
        byte[] columnVar1 = Bytes.toBytes("col_1_var");
        byte[] columnName2 = Bytes.toBytes("col_2");
        byte[] columnVar2 = Bytes.toBytes("col_2_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName1, columnVar1);
        put.addColumn(familyName, columnName2, columnVar2);
        table.put(put);

        {
            Get get = new Get(rowKey);
            get.addColumn(familyName, columnName1);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName1));
            assertEquals("col_1_var", value);

            value = Bytes.toString(result.getValue(familyName, columnName2));
            assertEquals(null, value);
        }

        {
            Get get = new Get(rowKey);
            get.addColumn(familyName, columnName1);
            get.addColumn(familyName, columnName2);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName1));
            assertEquals("col_1_var", value);

            value = Bytes.toString(result.getValue(familyName, columnName2));
            assertEquals("col_2_var", value);
        }

        {
            Get get = new Get(rowKey);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName1));
            assertEquals("col_1_var", value);

            value = Bytes.toString(result.getValue(familyName, columnName2));
            assertEquals("col_2_var", value);
        }
    }

    @Test
    public void testGetRowWithSpecialTs() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 2);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 2000, columnVar);
            table.put(put);
        }

        {
            Get get = new Get(rowKey);
            get.setTimeStamp(1000);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName));
            assertEquals("col_1_var", value);
            List<Cell> cells = result.getColumnCells(familyName, columnName);
            assertEquals(1, cells.size());
            assertEquals(1000, cells.get(0).getTimestamp());
        }

        {
            Get get = new Get(rowKey);
            get.setTimeStamp(2000);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName));
            assertEquals("col_1_var", value);
            List<Cell> cells = result.getColumnCells(familyName, columnName);
            assertEquals(1, cells.size());
            assertEquals(2000, cells.get(0).getTimestamp());
        }
    }

    @Test
    public void testGetRowWithMaxVersion() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 3);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 2000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1500, columnVar);
            table.put(put);
        }

        {
            Get get = new Get(rowKey);
            get.setMaxVersions(2);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName));
            assertEquals("col_1_var", value);
            List<Cell> cells = result.getColumnCells(familyName, columnName);
            assertEquals(2, cells.size());
            assertEquals(2000, cells.get(0).getTimestamp());
            assertEquals(1500, cells.get(1).getTimestamp());
        }
    }

    @Test
    public void testGetRowWithTimerange() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 4);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1500, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 2000, columnVar);
            table.put(put);
        }

        {
            Get get = new Get(rowKey);
            get.setTimeRange(1500, 2001);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName));
            assertEquals("col_1_var", value);
            List<Cell> cells = result.getColumnCells(familyName, columnName);
            assertEquals(1, cells.size());
            assertEquals(2000, cells.get(0).getTimestamp());
        }

        {
            Get get = new Get(rowKey);
            get.setTimeRange(1000, 1501);
            get.setMaxVersions(2);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(familyName, columnName));
            assertEquals("col_1_var", value);
            List<Cell> cells = result.getColumnCells(familyName, columnName);
            assertEquals(2, cells.size());
            assertEquals(1500, cells.get(0).getTimestamp());
            assertEquals(1000, cells.get(1).getTimestamp());
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithSpecialFamilyTimerange() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 5);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 2000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1500, columnVar);
            table.put(put);
        }

        {
            Get get = new Get(rowKey);
            get.setColumnFamilyTimeRange(familyName, 1500, 2001);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithOtherFamilyTimerange() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 6);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 2000, columnVar);
            table.put(put);
        }

        {
            Put put = new Put(rowKey);

            put.addColumn(familyName, columnName, 1500, columnVar);
            table.put(put);
        }

        {
            Get get = new Get(rowKey);
            get.setColumnFamilyTimeRange(Bytes.toBytes("new_family"), 1500, 2001);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithColumnFamilyTimeRange() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setColumnFamilyTimeRange(familyName, 0, 10000);
        table.get(get);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithClosestRowBefore() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setClosestRowBefore(true);
        table.get(get);
    }

    @Test
    public void testGetRowWithClosestRowBeforeFalse() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setClosestRowBefore(false);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithCacheBlocks() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setCacheBlocks(false);
        table.get(get);
    }

    @Test
    public void testGetRowWithCacheBlocksIsTrue() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setCacheBlocks(true);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithACL() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setACL("abc", new Permission());
        table.get(get);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithMaxResultsPerColumnFamily() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setMaxResultsPerColumnFamily(10);
        table.get(get);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithRowOffsetPerColumnFamily() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setRowOffsetPerColumnFamily(10);
        table.get(get);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithTimelineConsistency() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setConsistency(Consistency.TIMELINE);
        table.get(get);
    }

    @Test
    public void testGetRowWithStrongConsistency() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setConsistency(Consistency.STRONG);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithUncommitedIsolationLevel() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        table.get(get);
    }

    @Test
    public void testGetRowWithCommitedIsolationLevel() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setIsolationLevel(IsolationLevel.READ_COMMITTED);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetRowWithReplicaId() throws IOException {
        clean();
        byte[] rowKey = Bytes.toBytes(rowPrefix + 0);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        get.setReplicaId(100);
        table.get(get);
    }
}

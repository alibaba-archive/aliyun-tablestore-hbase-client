package com.alicloud.tablestore.hbase.scan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestScanRow {
    private static Table table = null;
    private static String familyName = null;
    private static String columnName = null;
    private static String columnValue = null;
    private static final String rowPrefix = "test_scan_row_";

    public TestScanRow() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        familyName = config.get("hbase.client.tablestore.family");
        columnName = "col_1";
        columnValue = "col_1_var";

        TableName tableName = TableName.valueOf(config.get("hbase.client.tablestore.table"));
        if (!connection.getAdmin().tableExists(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            connection.getAdmin().createTable(descriptor);
            TimeUnit.SECONDS.sleep(1);
        }
        table = connection.getTable(tableName);
    }

    private void putRow(String row, long ts) throws IOException {
        byte[] rowKey = Bytes.toBytes(row);
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes(this.columnName);
        byte[] columnVar = Bytes.toBytes(this.columnValue);

        Put put = new Put(rowKey);
        put.addColumn(familyName, columnName, ts, columnVar);
        table.put(put);
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
    public void testScanRowWithOne() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithTwo() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 4));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 3, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithAll() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 3, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithReverse() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setReversed(true);
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 3, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithCaching() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 1, 1000);
        putRow(row + 2, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setCaching(3);
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 1, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 2, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 3, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithTwoVersion() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 0, 2000);
        putRow(row + 0, 3000);
        putRow(row + 3, 1000);
        putRow(row + 3, 2000);
        putRow(row + 3, 3000);

        Scan scan = new Scan();
        scan.setMaxVersions(2);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 7));
        ResultScanner scanResult = table.getScanner(scan);

        {
            Result result = scanResult.next();
            assertTrue(result != null);
            assertEquals(row + 0, Bytes.toString(result.getRow()));
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            assertEquals(columnValue, value);

            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            assertEquals(2, cells.size());
            assertEquals(3000, cells.get(0).getTimestamp());
            assertEquals(2000, cells.get(1).getTimestamp());
        }

        {
            Result result = scanResult.next();
            assertTrue(result != null);
            assertEquals(row + 3, Bytes.toString(result.getRow()));
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            assertEquals(columnValue, value);

            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            assertEquals(2, cells.size());
            assertEquals(3000, cells.get(0).getTimestamp());
            assertEquals(2000, cells.get(1).getTimestamp());
        }

        Result result = scanResult.next();
        assertTrue(result == null);
    }

    @Test
    public void testScanRowWithTimerange() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 0, 2000);
        putRow(row + 0, 3000);
        putRow(row + 3, 1000);
        putRow(row + 3, 2000);
        putRow(row + 3, 3000);

        Scan scan = new Scan();
        scan.setMaxVersions(2);
        scan.setTimeRange(1000, 2001);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 7));
        ResultScanner scanResult = table.getScanner(scan);

        {
            Result result = scanResult.next();
            assertTrue(result != null);
            assertEquals(row + 0, Bytes.toString(result.getRow()));
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            assertEquals(columnValue, value);

            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            assertEquals(2, cells.size());
            assertEquals(2000, cells.get(0).getTimestamp());
            assertEquals(1000, cells.get(1).getTimestamp());
        }

        {
            Result result = scanResult.next();
            assertTrue(result != null);
            assertEquals(row + 3, Bytes.toString(result.getRow()));
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            assertEquals(columnValue, value);

            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            assertEquals(2, cells.size());
            assertEquals(2000, cells.get(0).getTimestamp());
            assertEquals(1000, cells.get(1).getTimestamp());
        }

        Result result = scanResult.next();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithACL() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setACL("abc", new Permission());
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithPartialResults() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setAllowPartialResults(true);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithAuthorization() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setAuthorizations(new Authorizations());
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithBatch() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithoutCacheBlock() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test
    public void testScanRowWithCacheBlock() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setCacheBlocks(true);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithFamilyTimerange() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setColumnFamilyTimeRange(Bytes.toBytes("abc"), 1000, 2000);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithTimline() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setConsistency(Consistency.TIMELINE);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test
    public void testScanRowWithStrong() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setConsistency(Consistency.STRONG);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithUncommitted() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithCommitted() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithLoadCfod() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setLoadColumnFamiliesOnDemand(true);
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithLoadCfodFalse() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setLoadColumnFamiliesOnDemand(false);
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithMaxResultSize() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setMaxResultSize(100);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithMaxFamilyResultSize() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setMaxResultsPerColumnFamily(100);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithReplication() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setReplicaId(100);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);
        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithRaw() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithRaw2() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setRaw(false);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithFamilyRowOffset() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setRowOffsetPerColumnFamily(100);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);
        scanResult.next();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithScanMetric() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setScanMetricsEnabled(true);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);
        scanResult.next();
    }

    @Test
    public void testScanRowWithPrefixFilter() throws IOException {
        clean();
        String row = rowPrefix + 5;
        putRow(rowPrefix + 4, 1000);
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);
        putRow(rowPrefix + 6, 1000);


        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(row));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 3, Bytes.toString(result.getRow()));
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);

        result = scanResult.next();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testScanRowWithSmall() throws IOException {
        clean();
        String row = rowPrefix + 0;
        putRow(row + 0, 1000);
        putRow(row + 3, 1000);

        Scan scan = new Scan();
        scan.setSmall(true);
        scan.setStartRow(Bytes.toBytes(row + 0));
        scan.setStopRow(Bytes.toBytes(row + 3));
        ResultScanner scanResult = table.getScanner(scan);

        Result result = scanResult.next();
        assertTrue(result != null);
        assertEquals(row + 0, Bytes.toString(result.getRow()));
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
        assertEquals(columnValue, value);
        scanResult.next();
    }
}

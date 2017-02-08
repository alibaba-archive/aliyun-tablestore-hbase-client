package com.alicloud.tablestore.hbase.delete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDeleteRow {
    private static Table table = null;
    private static String family = null;
    private static String columnName = null;
    private static String columnValue = null;
    private static final String rowPrefix = "test_delete_row_";

    public TestDeleteRow() throws IOException,InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        family = config.get("hbase.client.tablestore.family");
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

        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), ts, Bytes.toBytes(columnValue));
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
    public void testDeleteOneRow() throws IOException {
        clean();
        String row = rowPrefix;
        putRow(row + 0, 1000);

        Delete delete = new Delete(Bytes.toBytes(row + 0));
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(row + 0));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteLatestColumn() throws IOException {
        clean();
        String row = rowPrefix + 0;
        byte[] familyName = Bytes.toBytes(family);

        {
            byte[] rowKey = Bytes.toBytes(row);

            Put put = new Put(rowKey);
            put.addColumn(familyName, Bytes.toBytes("col_1"), 1000, Bytes.toBytes("var_1"));
            put.addColumn(familyName, Bytes.toBytes("col_2"), 1000, Bytes.toBytes("var_2"));
            table.put(put);
        }

        {
            byte[] rowKey = Bytes.toBytes(row);
            Put put = new Put(rowKey);
            put.addColumn(familyName, Bytes.toBytes("col_1"), 2000, Bytes.toBytes("var_1"));
            put.addColumn(familyName, Bytes.toBytes("col_2"), 2000, Bytes.toBytes("var_2"));
            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row + 0));
        delete.addColumn(familyName, Bytes.toBytes("col_1"));
        table.delete(delete);
    }

    @Test
    public void testDeleteSpecialColumnVersion() throws IOException {
        clean();
        byte[] familyName = Bytes.toBytes(family);


        {
            byte[] rowKey = Bytes.toBytes(rowPrefix);

            Put put = new Put(rowKey);
            put.addColumn(familyName, Bytes.toBytes("col_1"), 1000, Bytes.toBytes("var_1"));
            put.addColumn(familyName, Bytes.toBytes("col_2"), 1000, Bytes.toBytes("var_2"));
            table.put(put);
        }

        {
            byte[] rowKey = Bytes.toBytes(rowPrefix);
            Put put = new Put(rowKey);
            put.addColumn(familyName, Bytes.toBytes("col_1"), 2000, Bytes.toBytes("var_1"));
            put.addColumn(familyName, Bytes.toBytes("col_2"), 2000, Bytes.toBytes("var_2"));
            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addColumn(familyName, Bytes.toBytes("col_1"), 2000);
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(rowPrefix));
        get.setMaxVersions(2);
        Result result = table.get(get);
        List<Cell> cells = result.getColumnCells(familyName, Bytes.toBytes("col_1"));
        assertEquals(1, cells.size());
        assertEquals(1000, cells.get(0).getTimestamp());

        cells = result.getColumnCells(familyName, Bytes.toBytes("col_2"));
        assertEquals(2, cells.size());
        assertEquals(2000, cells.get(0).getTimestamp());
        assertEquals(1000, cells.get(1).getTimestamp());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithACL() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setACL("abc", new Permission());
        table.delete(delete);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithCellVisibility() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setCellVisibility(new CellVisibility("abc"));
        table.delete(delete);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithAttribute() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setAttribute("abc", Bytes.toBytes(10));
        table.delete(delete);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithDurability() throws IOException {
        clean();

        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setDurability(Durability.FSYNC_WAL);
        table.delete(delete);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithTTL() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setTTL(100);
        table.delete(delete);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithTimestamp() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.setTimestamp(100);
        table.delete(delete);
    }

    @Test
    public void testDeleteFamily() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addFamily(Bytes.toBytes(family));
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(rowPrefix));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testDeleteFamily2() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addFamily(Bytes.toBytes(family), 1000);
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(rowPrefix));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testDeleteWithFamilyVersion() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addFamilyVersion(Bytes.toBytes(family), 3000);
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(rowPrefix));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testDeleteWithColumns() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(columnName));
        table.delete(delete);

        Get get = new Get(Bytes.toBytes(rowPrefix));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteWithColumns2() throws IOException {
        clean();
        Delete delete = new Delete(Bytes.toBytes(rowPrefix));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(columnName), 1000);
        table.delete(delete);
    }
}
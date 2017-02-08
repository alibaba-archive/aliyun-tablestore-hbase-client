package com.alicloud.tablestore.hbase.delete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestCheckAndDeleteRow {
    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_check_and_delete_row_";

    public TestCheckAndDeleteRow() throws IOException, InterruptedException {
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
    public void testEqualAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete put = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"), put);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testEqualAndFail() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_11"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testNotExist() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("val_11"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testNotExist2() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_3"), null, delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testEqualAndSuccess2() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val_1"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testEqualAndFailed2() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val_3"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testNotEqualAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("val_3"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testNotEqualAndFailed() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("val_1"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testGreaterAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.GREATER, Bytes.toBytes("val_0"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testGreaterAndFailed() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete  delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.GREATER, Bytes.toBytes("val_1"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testGreaterOrEqualAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("val_0"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testGreaterOrEqualAndFailed() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("val_4"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testLessAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.LESS, Bytes.toBytes("val_5"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testLessAndFailed() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.LESS, Bytes.toBytes("val_0"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test
    public void testLessOrEqualAndSuccess() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("val_5"), delete);
        assertTrue(ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result == null);
    }

    @Test
    public void testLessOrEqualAndFailed() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row));

        boolean ret = table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("val_0"), delete);
        assertTrue(!ret);

        Get get = new Get(Bytes.toBytes(row));
        byte[] result = table.get(get).getRow();
        assertTrue(result != null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testOtherRow() throws IOException {
        clean();

        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"));

            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes(row + 1));

        table.checkAndDelete(Bytes.toBytes(row), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("val_1"), delete);
    }

}

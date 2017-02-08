package com.alicloud.tablestore.hbase.exist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExistRow {
    private static Table table = null;
    private static String family = null;
    private static String columnName = null;
    private static String columnValue = null;
    private static final String rowPrefix = "test_exist_row_";

    public TestExistRow() throws IOException, InterruptedException {
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
    public void testRowExist() throws IOException {
        clean();
        String row = rowPrefix;
        putRow(row + 0, 1000);

        Get get = new Get(Bytes.toBytes(row + 0));
        boolean exist = table.exists(get);
        assertTrue(exist);
    }

    @Test
    public void testRowsAllExist() throws IOException {
        clean();
        String row = rowPrefix;
        putRow(row + 0, 1000);
        putRow(row + 1, 1000);

        Get get1 = new Get(Bytes.toBytes(row + 0));
        Get get2 = new Get(Bytes.toBytes(row + 1));
        List<Get> gets = new ArrayList<Get>();
        gets.add(get1);
        gets.add(get2);

        boolean[] exists = table.existsAll(gets);
        assertEquals(2, exists.length);
        assertTrue(exists[0]);
        assertTrue(exists[1]);
    }

    @Test
    public void testRowsPartExist() throws IOException {
        clean();
        String row = rowPrefix;
        putRow(row + 0, 1000);

        Get get1 = new Get(Bytes.toBytes(row + 0));
        Get get2 = new Get(Bytes.toBytes(row + 1));
        List<Get> gets = new ArrayList<Get>();
        gets.add(get1);
        gets.add(get2);

        boolean[] exists = table.existsAll(gets);
        assertEquals(2, exists.length);
        assertTrue(exists[0]);
        assertTrue(!exists[1]);
    }

    @Test
    public void testRowNotExist() throws IOException {
        clean();
        String row = rowPrefix;

        Get get = new Get(Bytes.toBytes(row + 0));
        boolean exist = table.exists(get);
        assertTrue(!exist);
    }
}
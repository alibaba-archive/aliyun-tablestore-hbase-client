package com.alicloud.tablestore.hbase.batch;

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

public class TestBatchRow {
    private static Table table = null;
    private static String family = null;
    private static String columnName = null;
    private static String columnValue = null;
    private static final String rowPrefix = "test_exist_row_";

    public TestBatchRow() throws IOException,InterruptedException {
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
    public void testOnePutGet() throws IOException,InterruptedException {
        clean();
        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            List<Row> rows = new ArrayList<Row>();
            rows.add(put);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(1, results.length);
        }

        {
            Get get = new Get(Bytes.toBytes(row));
            List<Row> rows = new ArrayList<Row>();
            rows.add(get);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(1, results.length);
            Result result = (Result)results[0];
            assertTrue(result.getRow() != null);
        }
    }

    @Test
    public void testTowPutGet() throws IOException,InterruptedException {
        clean();
        String row = rowPrefix;
        {
            Put put1 = new Put(Bytes.toBytes(row + 0));
            put1.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            Put put2 = new Put(Bytes.toBytes(row + 1));
            put2.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            List<Row> rows = new ArrayList<Row>();
            rows.add(put1);
            rows.add(put2);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(2, results.length);
        }

        {
            Get get1 = new Get(Bytes.toBytes(row + 0));
            Get get2 = new Get(Bytes.toBytes(row + 1));
            List<Row> rows = new ArrayList<Row>();
            rows.add(get1);
            rows.add(get2);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(2, results.length);
            Result result = (Result)results[0];
            assertTrue(result.getRow() != null);

            result = (Result)results[1];
            assertTrue(result.getRow() != null);
        }
    }

    @Test
    public void testPutGetDelete() throws IOException,InterruptedException {
        clean();
        String row = rowPrefix;
        {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            Get get = new Get(Bytes.toBytes(row + 1));

            Delete delete = new Delete(Bytes.toBytes(row + 2));

            List<Row> rows = new ArrayList<Row>();
            rows.add(put);
            rows.add(get);
            rows.add(delete);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(3, results.length);
        }
    }

    @Test
    public void testPutGetDelete2() throws IOException,InterruptedException {
        clean();

        // put:0, 1
        String row = rowPrefix;
        {
            Put put1 = new Put(Bytes.toBytes(row + 0));
            put1.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            Put put2 = new Put(Bytes.toBytes(row + 1));
            put2.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            List<Row> rows = new ArrayList<Row>();
            rows.add(put1);
            rows.add(put2);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(2, results.length);
        }

        //put:3, Get:0, Delete:1
        {
            Put put = new Put(Bytes.toBytes(row + 3));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            Get get = new Get(Bytes.toBytes(row + 0));

            Delete delete = new Delete(Bytes.toBytes(row + 1));

            List<Row> rows = new ArrayList<Row>();
            rows.add(put);
            rows.add(get);
            rows.add(delete);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(3, results.length);
            Result getResult = (Result)results[1];
            assertTrue(getResult.getRow() != null);
        }

        // Get:3,1
        {
            Get get1 = new Get(Bytes.toBytes(row + 3));
            Get get2 = new Get(Bytes.toBytes(row + 1));
            List<Row> rows = new ArrayList<Row>();
            rows.add(get1);
            rows.add(get2);

            Object[] results = new Object[rows.size()];
            table.batch(rows, results);

            assertEquals(2, results.length);
            Result result = (Result)results[0];
            assertTrue(result.getRow() != null);

            result = (Result)results[1];
            assertTrue(result.getRow() == null);
        }
    }
}

package com.alicloud.tablestore.hbase.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestPutRow {
    private static Table table = null;
    private static String familyName = null;

    public TestPutRow() throws IOException, InterruptedException {
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

    @Test
    public void testPutRowWithOneColumn() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk0");
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
    public void testPutRowWithOneNull() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk0");
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, null);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("", value);
    }

    @Test
    public void testPutRowWithTwoColumn() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk1");
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName1 = Bytes.toBytes("col_1");
        byte[] columnVar1 = Bytes.toBytes("col_1_var");
        byte[] columnName2 = Bytes.toBytes("col_2");
        byte[] columnVar2 = Bytes.toBytes("col_2_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName1, columnVar1);
        put.addColumn(familyName, columnName2, columnVar2);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName1));
        assertEquals("col_1_var", value);

        value = Bytes.toString(result.getValue(familyName, columnName2));
        assertEquals("col_2_var", value);
    }

    @Test
    public void testPutRowWithSpecialTs() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk2");
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, 1000, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
        Cell cell = result.getColumnLatestCell(familyName, columnName);
        assertEquals(1000, cell.getTimestamp());
    }

    @Test
    public void testPutRowWithTwoColumnTs() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk3");
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName1 = Bytes.toBytes("col_1");
        byte[] columnVar1 = Bytes.toBytes("col_1_var");
        byte[] columnName2 = Bytes.toBytes("col_2");
        byte[] columnVar2 = Bytes.toBytes("col_2_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName1, 1000, columnVar1);
        put.addColumn(familyName, columnName2, 2000, columnVar2);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName1));
        assertEquals("col_1_var", value);
        Cell cell = result.getColumnLatestCell(familyName, columnName1);
        assertEquals(1000, cell.getTimestamp());

        value = Bytes.toString(result.getValue(familyName, columnName2));
        assertEquals("col_2_var", value);
        cell = result.getColumnLatestCell(familyName, columnName2);
        assertEquals(2000, cell.getTimestamp());
    }

    @Test
    public void testPutRowWithoutSpecialTs() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk4");
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
        Cell cell = result.getColumnLatestCell(familyName, columnName);
        assertEquals(Time.now(), cell.getTimestamp(), 60 * 1000);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testPutRowWithTowFamily() throws IOException {
        byte[] rowKey = Bytes.toBytes("pk5");
        byte[] familyName = Bytes.toBytes(this.familyName);
        byte[] columnName = Bytes.toBytes("col_1");
        byte[] columnVar = Bytes.toBytes("col_1_var");

        Put put = new Put(rowKey);

        put.addColumn(familyName, columnName, columnVar);
        put.addColumn(Bytes.toBytes(this.familyName + 1), columnName, columnVar);
        table.put(put);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(familyName, columnName));
        assertEquals("col_1_var", value);
    }
}

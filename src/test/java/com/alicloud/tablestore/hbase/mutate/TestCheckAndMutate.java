package com.alicloud.tablestore.hbase.mutate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCheckAndMutate {
    private static Table table = null;
    private static String familyName = null;

    public TestCheckAndMutate() throws IOException, InterruptedException {
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
    public void testCheckNullSucceeded() throws IOException {
        clean();

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(put);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.EQUAL,
                null, mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
    }

    @Test
    public void testCheckWithOnePutSucceeded() throws IOException {
        clean();

        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(put);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("col_1_var"), mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3")));
        assertEquals("col_3_var", value);
    }

    @Test
    public void testCheckWithOnePutFailed() throws IOException {
        clean();

        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(put);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.NOT_EQUAL,
                Bytes.toBytes("col_1_var"), mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3")));
        assertTrue(value == null);
    }

    @Test
    public void testMutationWithOneDelete() throws IOException {
        clean();

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
        table.put(put);


        Delete delete = new Delete(Bytes.toBytes("pk0"));
        delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(delete);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes("col_1_var"), mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        byte[] col2 = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
        assertTrue(col2 == null);
    }

    @Test
    public void testMutationWithPutAndDelete() throws IOException {
        clean();

        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes("pk0"));
        delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(delete);
        mutaions.add(put);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.GREATER,
                Bytes.toBytes("col_1_var"), mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
        assertEquals("col_2_var", value);
        byte[] col3 = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
        assertTrue(col3 == null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testCheckARowAndMutateBRow() throws IOException {
        clean();

        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes("pk1"));
        delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));

        Put put = new Put(Bytes.toBytes("pk1"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk1"));
        mutaions.add(delete);
        mutaions.add(put);

        table.checkAndMutate(Bytes.toBytes("pk0"), Bytes.toBytes(familyName), Bytes.toBytes("col_1"), CompareFilter.CompareOp.GREATER,
                Bytes.toBytes("col_1_var"), mutaions);
    }
}

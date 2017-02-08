package com.alicloud.tablestore.hbase.mutate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMutateRow {
    private static Table table = null;
    private static String familyName = null;

    public TestMutateRow() throws IOException, InterruptedException {
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
    public void testMutationWithOnePut() throws IOException {
        clean();

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(put);

        table.mutateRow(mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
    }

    @Test
    public void testMutationWithOnePut2() throws IOException {
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

        table.mutateRow(mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3")));
        assertEquals("col_3_var", value);
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

        table.mutateRow(mutaions);

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

        table.mutateRow(mutaions);

        Get get = new Get(Bytes.toBytes("pk0"));
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
        assertEquals("col_1_var", value);
        byte[] col2 = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
        assertTrue(col2 == null);
        value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3")));
        assertEquals("col_3_var", value);
    }

    @Test(expected=WrongRowIOException.class)
    public void testMutationWithNotEqualRow() throws IOException {
        clean();

        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        Delete delete = new Delete(Bytes.toBytes("pk1"));
        delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));

        Put put = new Put(Bytes.toBytes("pk0"));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

        RowMutations mutaions = new RowMutations(Bytes.toBytes("pk0"));
        mutaions.add(delete);
        mutaions.add(put);

        table.mutateRow(mutaions);
    }
}

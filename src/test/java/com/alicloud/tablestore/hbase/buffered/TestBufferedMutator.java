package com.alicloud.tablestore.hbase.buffered;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBufferedMutator {
    private static BufferedMutator mutator = null;
    private static Table table = null;
    private static String familyName = null;

    public TestBufferedMutator() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        familyName = config.get("hbase.client.tablestore.family");

        TableName tableName = TableName.valueOf(config.get("hbase.client.tablestore.table"));
        if (!connection.getAdmin().tableExists(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            connection.getAdmin().createTable(descriptor);
            TimeUnit.SECONDS.sleep(1);
        }
        mutator = connection.getBufferedMutator(tableName);
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
    public void testPutRow() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            mutator.mutate(put);
        }

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }

        mutator.flush();

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testDeleteRowWithOneColumn() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Delete delete = new Delete(Bytes.toBytes("pk0"));
            delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            mutator.mutate(delete);
        }

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }

        mutator.flush();

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertTrue(value == null);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }
    }

    @Test
    public void testDeleteRow() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Delete delete = new Delete(Bytes.toBytes("pk0"));
            mutator.mutate(delete);
        }

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }

        mutator.flush();

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testPutAndDeleteRow() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Delete delete = new Delete(Bytes.toBytes("pk0"));
            delete.addColumns(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));

            Put put = new Put(Bytes.toBytes("pk0"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));

            Put put2 = new Put(Bytes.toBytes("pk1"));
            put2.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));

            List<Mutation> list = new ArrayList<Mutation>();
            list.add(delete);
            list.add(put);
            list.add(put2);
            mutator.mutate(list);
        }

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }

        {
            Get get = new Get(Bytes.toBytes("pk1"));
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }

        mutator.flush();

        {
            Get get = new Get(Bytes.toBytes("pk0"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertTrue(value == null);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_3")));
            assertEquals("col_3_var", value);
        }


        {
            Get get = new Get(Bytes.toBytes("pk1"));
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_4")));
            assertEquals("col_4_var", value);
        }
    }

    //@Test
    public void testMultiperPutAndDeleteRow() throws IOException {
        clean();
        {
            List<Put> puts = new ArrayList<Put>();
            int i = 0;
            while (i++ < 1000) {
                Put put = new Put(Bytes.toBytes("pk" + i));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
                puts.add(put);
            }
            mutator.mutate(puts);
            mutator.flush();
        }

        {
            int i = 0;
            while (i++ < 20000) {
                if (i % 20 == 0) {
                    Delete delete = new Delete(Bytes.toBytes("pk" + i/20));
                    mutator.mutate(delete);
                } else {
                    Put put = new Put(Bytes.toBytes("pk" + i + 1000));
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
                    mutator.mutate(Collections.singletonList(put));
                }
            }
        }

        {
            Scan scan = new Scan();
            ResultScanner result = table.getScanner(scan);

            int total = 0;
            while (result.next() != null) {
                total++;
            }
            assertTrue(total <= 19000);
        }

        mutator.flush();

        {
            Scan scan = new Scan();
            ResultScanner result = table.getScanner(scan);

            int total = 0;
            while (result.next() != null) {
                total++;
            }

            assertEquals(19000, total);
        }
    }
}

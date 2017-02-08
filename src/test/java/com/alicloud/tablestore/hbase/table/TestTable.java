package com.alicloud.tablestore.hbase.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestTable {
    private static Table table = null;
    private static String familyName = null;

    public TestTable() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        familyName = config.get("hbase.client.tablestore.family");

        TableName tableName = TableName.valueOf("____only_for_sdk_test_table_name");
        if (!connection.getAdmin().tableExists(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            connection.getAdmin().createTable(descriptor);
            TimeUnit.SECONDS.sleep(1);
        }
        table = connection.getTable(tableName);
    }

    @Test
    public void testGetDescriptor() throws IOException {
        HTableDescriptor descriptor = table.getTableDescriptor();

        assertEquals(table.getName().getNameAsString(), descriptor.getNameAsString());

        Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
        assertEquals(1, columnDescriptors.size());
        assertEquals(1, columnDescriptors.iterator().next().getMaxVersions());
        assertEquals(Integer.MAX_VALUE, columnDescriptors.iterator().next().getTimeToLive());
    }
}

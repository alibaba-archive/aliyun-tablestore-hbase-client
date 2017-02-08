package com.alicloud.tablestore.hbase.admin;

import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.hbase.ColumnMapping;
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
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAdmin {
    private static Admin admin = null;
    private static String tableNamePrefix = "____only_for_sdk_test_table_name_prefix_";

    public TestAdmin() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }

    public void clean() throws IOException {
        admin.deleteTables(tableNamePrefix + ".*");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
        }
    }

    @Test
    public void testTableExistSucceeded() throws IOException {
        clean();

        String tableName = tableNamePrefix + "0";
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        admin.createTable(descriptor);

        TableName table = TableName.valueOf(tableName);
        boolean exist = admin.tableExists(table);
        assertTrue(exist);
    }

    @Test
    public void testTableExistFailed() throws IOException {
        clean();

        String tableName = tableNamePrefix + "1";
        TableName table = TableName.valueOf(tableName);
        boolean exist = admin.tableExists(table);
        assertTrue(!exist);
    }

    @Test
    public void testCreateTable() throws IOException {
        clean();

        String tableName = tableNamePrefix + "0";
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        admin.createTable(descriptor);

        // check name
        descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        assertEquals(tableName, descriptor.getTableName().getNameAsString());

        // check maxVersion and TimeToLive
        Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
        assertEquals(1, columnDescriptors.size());
        HColumnDescriptor columnDescriptor = columnDescriptors.iterator().next();
        assertEquals(1, columnDescriptor.getMaxVersions());
        assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
        assertTrue(columnDescriptor.isBlockCacheEnabled());

        // check family name
        ColumnMapping columnMapping = new ColumnMapping(tableName, admin.getConfiguration());
        assertEquals(Bytes.toString(columnMapping.getFamilyNameBytes()), Bytes.toString(columnDescriptor.getName()));
    }

    @Test
    public void testCreateTableWithMaxVersion() throws IOException {
        clean();
        String tableName = tableNamePrefix + "0";
        ColumnMapping columnMapping = new ColumnMapping(tableName, admin.getConfiguration());
        {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toString(columnMapping.getFamilyNameBytes()));
            columnDescriptor.setMaxVersions(10);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
        }

        // check name
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        assertEquals(tableName, descriptor.getTableName().getNameAsString());

        // check maxVersion and TimeToLive
        Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
        assertEquals(1, columnDescriptors.size());
        HColumnDescriptor columnDescriptor = columnDescriptors.iterator().next();
        assertEquals(10, columnDescriptor.getMaxVersions());
        assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
        assertTrue(columnDescriptor.isBlockCacheEnabled());

        // check family name
        assertEquals(Bytes.toString(columnMapping.getFamilyNameBytes()), Bytes.toString(columnDescriptor.getName()));
    }

    @Test
    public void testCreateTableWithTTL() throws IOException {
        clean();
        String tableName = tableNamePrefix + "0";
        ColumnMapping columnMapping = new ColumnMapping(tableName, admin.getConfiguration());
        {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toString(columnMapping.getFamilyNameBytes()));
            columnDescriptor.setTimeToLive(86401);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
        }

        // check name
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        assertEquals(tableName, descriptor.getTableName().getNameAsString());

        // check maxVersion and TimeToLive
        Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
        assertEquals(1, columnDescriptors.size());
        HColumnDescriptor columnDescriptor = columnDescriptors.iterator().next();
        assertEquals(1, columnDescriptor.getMaxVersions());
        assertEquals(86401, columnDescriptor.getTimeToLive());
        assertTrue(columnDescriptor.isBlockCacheEnabled());

        // check family name
        assertEquals(Bytes.toString(columnMapping.getFamilyNameBytes()), Bytes.toString(columnDescriptor.getName()));
    }

    @Test
    public void testListTable() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        HTableDescriptor[] descriptors = admin.listTables();
        assertTrue(descriptors.length > 2);

        for (HTableDescriptor descriptor : descriptors) {
            if (descriptor.getNameAsString().equals(tableNamePrefix + "abc")) {
                ColumnMapping columnMapping = new ColumnMapping(descriptor.getNameAsString(), admin.getConfiguration());

                HColumnDescriptor columnDescriptor = descriptor.getFamily(columnMapping.getFamilyNameBytes());
                assertEquals(1, columnDescriptor.getMaxVersions());
                assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
            } else if (descriptor.getNameAsString().equals(tableNamePrefix + "axy")) {
                ColumnMapping columnMapping = new ColumnMapping(descriptor.getNameAsString(), admin.getConfiguration());

                HColumnDescriptor columnDescriptor = descriptor.getFamily(columnMapping.getFamilyNameBytes());
                assertEquals(1, columnDescriptor.getMaxVersions());
                assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
            }
        }
    }

    @Test
    public void testListTableWithRegex() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        HTableDescriptor[] descriptors = admin.listTables(tableNamePrefix + "ab.*");
        assertEquals(1, descriptors.length);

        {
            HTableDescriptor descriptor = descriptors[0];
            assertEquals(tableNamePrefix + "abc", descriptor.getNameAsString());
            ColumnMapping columnMapping = new ColumnMapping(descriptor.getNameAsString(), admin.getConfiguration());

            HColumnDescriptor columnDescriptor = descriptor.getFamily(columnMapping.getFamilyNameBytes());
            assertEquals(1, columnDescriptor.getMaxVersions());
            assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
        }
    }

    @Test
    public void testListTableWithPattern() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        String regex = tableNamePrefix + "ax.*";
        Pattern pattern = Pattern.compile(regex);
        HTableDescriptor[] descriptors = admin.listTables(pattern);
        assertEquals(1, descriptors.length);

        {
            HTableDescriptor descriptor = descriptors[0];
            assertEquals(tableNamePrefix + "axy", descriptor.getNameAsString());
            ColumnMapping columnMapping = new ColumnMapping(descriptor.getNameAsString(), admin.getConfiguration());

            HColumnDescriptor columnDescriptor = descriptor.getFamily(columnMapping.getFamilyNameBytes());
            assertEquals(1, columnDescriptor.getMaxVersions());
            assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
        }
    }

    @Test
    public void testListTableNames() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        TableName[] tableNames = admin.listTableNames();
        assertTrue(tableNames.length>= 2);
    }

    @Test
    public void testListTableNamesWithRegex() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        TableName[] tableNames = admin.listTableNames(tableNamePrefix + "ab.*");
        assertEquals(1, tableNames.length);
        assertEquals(tableNamePrefix + "abc", tableNames[0].getNameAsString());
    }

    @Test
    public void testListTableNamesWithPattern() throws IOException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        Pattern pattern = Pattern.compile(tableNamePrefix + "ax.*");
        TableName[] tableNames = admin.listTableNames(pattern);
        assertEquals(1, tableNames.length);
        assertEquals(tableNamePrefix + "axy", tableNames[0].getNameAsString());
    }

    @Test
    public void testDeleteTablesWithRegex() throws IOException, InterruptedException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        HTableDescriptor[] descriptors = admin.deleteTables(tableNamePrefix + "ax.*");
        assertEquals(1, descriptors.length);
        assertEquals(tableNamePrefix + "axy", descriptors[0].getNameAsString());

        TimeUnit.SECONDS.sleep(1);
        assertTrue(!admin.tableExists(TableName.valueOf(tableNamePrefix + "axy")));
    }

    @Test
    public void testDeleteTablesWithPattern() throws IOException, InterruptedException {
        clean();

        {
            String tableName = tableNamePrefix + "abc";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        {
            String tableName = tableNamePrefix + "axy";
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);
        }

        Pattern pattern = Pattern.compile(tableNamePrefix + "ax.*");
        HTableDescriptor[] descriptors = admin.deleteTables(pattern);
        assertEquals(1, descriptors.length);
        assertEquals(tableNamePrefix + "axy", descriptors[0].getNameAsString());

        TimeUnit.SECONDS.sleep(1);
        assertTrue(!admin.tableExists(TableName.valueOf(tableNamePrefix + "axy")));
    }

    @Test
    public void testEnableTable() throws IOException {
        clean();

        String tableName = tableNamePrefix + "abc";
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        admin.createTable(descriptor);
        assertTrue(admin.isTableAvailable(TableName.valueOf(tableName)));
        assertTrue(admin.isTableEnabled(TableName.valueOf(tableName)));

        admin.disableTable(TableName.valueOf(tableName));
        assertTrue(admin.isTableAvailable(TableName.valueOf(tableName)));
        assertTrue(!admin.isTableEnabled(TableName.valueOf(tableName)));

        admin.enableTable(TableName.valueOf(tableName));
        assertTrue(admin.isTableAvailable(TableName.valueOf(tableName)));
        assertTrue(admin.isTableEnabled(TableName.valueOf(tableName)));
    }

    @Test
    public void testUpdateTable() throws IOException {
        clean();

        String tableName = tableNamePrefix + "jsq";
        ColumnMapping columnMapping = new ColumnMapping(tableName, admin.getConfiguration());

        // create table
        {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(descriptor);

        }

        // check table descriptor
        {
            // check name
            HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
            assertEquals(tableName, descriptor.getTableName().getNameAsString());

            // check maxVersion and TimeToLive
            Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
            assertEquals(1, columnDescriptors.size());
            HColumnDescriptor columnDescriptor = columnDescriptors.iterator().next();
            assertEquals(1, columnDescriptor.getMaxVersions());
            assertEquals(Integer.MAX_VALUE, columnDescriptor.getTimeToLive());
            assertTrue(columnDescriptor.isBlockCacheEnabled());

            // check family name
            assertEquals(Bytes.toString(columnMapping.getFamilyNameBytes()), Bytes.toString(columnDescriptor.getName()));
        }

        // update table
        {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnMapping.getFamilyNameBytes());
            columnDescriptor.setMaxVersions(2);
            columnDescriptor.setTimeToLive(86400);

            admin.modifyColumn(TableName.valueOf(tableName), columnDescriptor);
        }

        // check table descriptor after update
        {
            // check name
            HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
            assertEquals(tableName, descriptor.getTableName().getNameAsString());

            // check maxVersion and TimeToLive
            Collection<HColumnDescriptor> columnDescriptors = descriptor.getFamilies();
            assertEquals(1, columnDescriptors.size());
            HColumnDescriptor columnDescriptor = columnDescriptors.iterator().next();
            assertEquals(2, columnDescriptor.getMaxVersions());
            assertEquals(86400, columnDescriptor.getTimeToLive());
            assertTrue(columnDescriptor.isBlockCacheEnabled());

            // check family name
            assertEquals(Bytes.toString(columnMapping.getFamilyNameBytes()), Bytes.toString(columnDescriptor.getName()));
        }
    }

}
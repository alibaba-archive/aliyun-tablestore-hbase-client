package com.alicloud.tablestore.hbase.region;


import com.alicloud.tablestore.adaptor.client.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRegionLocator {
    private static RegionLocator regionLocator = null;
    private static Admin admin = null;
    private static Table table = null;
    private static String tableName = "____only_for_sdk_test_region_locator";

    public TestRegionLocator() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
        admin = connection.getAdmin();
        table = connection.getTable(TableName.valueOf(tableName));
    }

    public void clean() throws IOException {
        admin.deleteTables(tableName);
    }

    @Test
    public void testGetRegionLocationWithEmptyTable() throws IOException {
        clean();

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        admin.createTable(descriptor);

        HRegionLocation region = regionLocator.getRegionLocation(Bytes.toBytes(10));
        assertEquals(tableName, region.getRegionInfo().getTable().getNameAsString());
        assertTrue(Arrays.equals(HConstants.EMPTY_START_ROW, region.getRegionInfo().getStartKey()));
        assertTrue(Arrays.equals(HConstants.EMPTY_END_ROW, region.getRegionInfo().getEndKey()));
        assertEquals(0, region.getSeqNum());
    }

    // splited in "pk_1" position
    public void testGetRegionLocationWithOneRow() throws IOException {
        clean();

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        admin.createTable(descriptor);

        Put put = new Put(Bytes.toBytes("pk_1"));
        put.addColumn(Bytes.toBytes(""), Bytes.toBytes("col_1"), Bytes.toBytes("col_2"));
        table.put(put);

        byte[][] starts = regionLocator.getStartKeys();
        assertEquals(2, starts.length);
        assertTrue(Arrays.equals(HConstants.EMPTY_START_ROW, starts[0]));
        assertTrue(Arrays.equals(Bytes.toBytes("pk_1"), starts[1]));

        byte[][] ends = regionLocator.getEndKeys();
        assertEquals(2, ends.length);
        assertTrue(Arrays.equals(Bytes.toBytes("pk_1"), ends[0]));
        assertTrue(Arrays.equals(HConstants.EMPTY_END_ROW, ends[1]));
    }
}

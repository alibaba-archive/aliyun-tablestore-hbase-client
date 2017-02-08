package com.alicloud.tablestore.hbase.get;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestColumnPaginationFilter {
    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_filter_";

    public TestColumnPaginationFilter() throws IOException, InterruptedException {
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
    public void testGet() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnPaginationFilter(1, 2);
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(1, cells.size());
            assertEquals("col_3_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(0, cells.size());
        }
    }

    @Test
    public void testScan() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Scan scan = new Scan();
            Filter filter = new ColumnPaginationFilter(1, 2);
            scan.setFilter(filter);
            scan.setStartRow(Bytes.toBytes(rowPrefix));
            scan.setStopRow(Bytes.toBytes(rowPrefix + 5));
            ResultScanner scanResult = table.getScanner(scan);

            Result result = scanResult.next();
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(1, cells.size());
            assertEquals("col_3_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(0, cells.size());
        }
    }

    @Test
    public void testResultLessThanLimit() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnPaginationFilter(5, 3);
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(1, cells.size());
            assertEquals("col_4_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));
        }
    }

    @Test
    public void testStartRow() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnPaginationFilter(1, Bytes.toBytes("col_2"));
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(1, cells.size());
            assertEquals("col_2_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(0, cells.size());
        }
    }

    @Test
    public void testStartRowByScan() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Scan scan = new Scan();
            Filter filter = new ColumnPaginationFilter(1, Bytes.toBytes("col_2"));
            scan.setFilter(filter);
            scan.setStartRow(Bytes.toBytes(rowPrefix));
            scan.setStopRow(Bytes.toBytes(rowPrefix + 5));
            ResultScanner scanResult = table.getScanner(scan);

            Result result = scanResult.next();

            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(1, cells.size());
            assertEquals("col_2_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(0, cells.size());
        }
    }

    @Test
    public void testStartRowIsNotExist() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_3"), Bytes.toBytes("col_3_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_4"), Bytes.toBytes("col_4_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new ColumnPaginationFilter(1, Bytes.toBytes("col_3_not_exist"));
            get.setFilter(filter);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_2"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_3"));
            assertEquals(0, cells.size());

            cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_4"));
            assertEquals(1, cells.size());
            assertEquals("col_4_var", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));
        }
    }
}

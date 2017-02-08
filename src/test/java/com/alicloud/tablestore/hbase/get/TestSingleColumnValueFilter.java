package com.alicloud.tablestore.hbase.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSingleColumnValueFilter {
    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_filter_";

    public TestSingleColumnValueFilter() throws IOException, InterruptedException {
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
    public void testFilterWithEqualSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithEqualFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var2"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithNotEqualSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_1_var2"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithNotEqualFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_1_var"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithGreaterSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, Bytes.toBytes("col_1_vaa"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithGreaterFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, Bytes.toBytes("col_1_vaz"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithGreaterEqualSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("col_1_vaa"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithGreaterEqualFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("col_1_vaz"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithLessSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.LESS, Bytes.toBytes("col_1_vaz"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithLessFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.LESS, Bytes.toBytes("col_1_vaa"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithLessEqualSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("col_1_vaz"));
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithLessEqualFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("col_1_vaa"));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithFilterIfMissingSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_3"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            filter.setFilterIfMissing(false);
            get.setFilter(filter);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
        }
    }

    @Test
    public void testFilterWithFilterIfMissingFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_3"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            filter.setFilterIfMissing(true);
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithLatestVersionTrue() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), 1000, Bytes.toBytes("abc"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), 2000, Bytes.toBytes("col"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("abc"));
            filter.setLatestVersionOnly(true);
            get.setFilter(filter);
            get.setMaxVersions(2);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(2, cells.size());
            assertEquals("col", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));
            assertEquals("abc", Bytes.toString(cells.get(1).getValueArray(), cells.get(1).getValueOffset(), cells.get(1).getValueLength()));
        }
    }

    @Test
    public void testFilterWithLatestVersionFalse() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), 1000, Bytes.toBytes("abc"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), 2000, Bytes.toBytes("col"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("abc"));
            filter.setLatestVersionOnly(false);
            get.setFilter(filter);
            get.setMaxVersions(2);
            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(familyName), Bytes.toBytes("col_1"));
            assertEquals(2, cells.size());
            assertEquals("col", Bytes.toString(cells.get(0).getValueArray(), cells.get(0).getValueOffset(), cells.get(0).getValueLength()));
            assertEquals("abc", Bytes.toString(cells.get(1).getValueArray(), cells.get(1).getValueOffset(), cells.get(1).getValueLength()));
        }
    }

    @Test
    public void testFilterWithBinaryComparatorSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("2")));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testFilterWithBinaryComparatorFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("10000")));
            get.setFilter(filter);
            Result result = table.get(get);
            assertTrue(result.getRow() != null);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testFilterWithBitComparatorFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new BitComparator(Bytes.toBytes("10000"), BitComparator.BitwiseOp.AND));
            get.setFilter(filter);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testFilterWithBinaryPrefixComparatorFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(Bytes.toBytes("10000")));
            get.setFilter(filter);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testFilterWithNullComparatorFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new NullComparator());
            get.setFilter(filter);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testFilterWithRegexComparatorFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new RegexStringComparator("abc*"));
            get.setFilter(filter);
            table.get(get);
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testFilterWithLongComparatorSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("1900"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.GREATER, new LongComparator(999));
            get.setFilter(filter);
            table.get(get);
        }
    }
}

package com.alicloud.tablestore.hbase.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFilterList {
    private static Table table = null;
    private static String familyName = null;
    private static final String rowPrefix = "test_filter_list_";

    public TestFilterList() throws IOException, InterruptedException {
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
    public void testTwoFilterWithDefaultOperator() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_2"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_2_var"));
            FilterList filterList = new FilterList();
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            get.setFilter(filterList);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }
    }

    @Test
    public void testTwoFilterWithMustAllPassSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_2"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_2_var"));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            get.setFilter(filterList);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }
    }

    @Test
    public void testTwoFilterWithMustAllPassFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_2"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_2_var"));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            get.setFilter(filterList);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }

    @Test
    public void testTwoFilterWithMustOnePassSucceeded() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("col_1_var"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_2"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_2_var"));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            get.setFilter(filterList);
            Result result = table.get(get);
            String value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_1")));
            assertEquals("col_1_var", value);
            value = Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes("col_2")));
            assertEquals("col_2_var", value);
        }
    }

    @Test
    public void testTwoFilterWithMustOnePassFailed() throws IOException {
        clean();
        {
            Put put = new Put(Bytes.toBytes(rowPrefix));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_1"), Bytes.toBytes("col_1_var"));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("col_2"), Bytes.toBytes("col_2_var"));
            table.put(put);
        }

        {
            Get get = new Get(Bytes.toBytes(rowPrefix));
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_1"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_1_var"));
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes("col_2"),
                    CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("col_2_var"));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            get.setFilter(filterList);
            Result result = table.get(get);
            assertTrue(result.getRow() == null);
        }
    }
}

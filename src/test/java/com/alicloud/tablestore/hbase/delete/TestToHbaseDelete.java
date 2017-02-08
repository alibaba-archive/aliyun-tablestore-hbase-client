package com.alicloud.tablestore.hbase.delete;

import com.alicloud.tablestore.adaptor.struct.ODelete;
import com.alicloud.tablestore.hbase.ColumnMapping;
import com.alicloud.tablestore.hbase.ElementConvertor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestToHbaseDelete {
    private static Table table = null;
    private static String family = null;
    private ColumnMapping tablestoreColumnMapping;

    public TestToHbaseDelete() throws IOException,InterruptedException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        tablestoreColumnMapping = new ColumnMapping("table-1", connection.getConfiguration());
        family = config.get("hbase.client.tablestore.family");
    }

    @Test
    public void testDeleteOneRow() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("pk"));

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        assertEquals(delete.toJSON(), target.toJSON());
    }

    @Test
    public void testDeleteSpecialColumnVersion() throws IOException {
        byte[] familyName = Bytes.toBytes(family);

        Delete delete = new Delete(Bytes.toBytes("pk2"));
        delete.addColumn(familyName, Bytes.toBytes("col_1"), 2000);

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        assertEquals(delete.toJSON(), target.toJSON());
    }

    @Test
    public void testDeleteFamily() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("pk3"));
        delete.addFamily(Bytes.toBytes(family));

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        target.addFamily(Bytes.toBytes(family)); // family is ignore
        assertEquals(delete.toJSON(), target.toJSON());
    }

    @Test
    public void testDeleteFamily2() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("pk4"));
        delete.addFamily(Bytes.toBytes(family), 1000);

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        target.addFamily(Bytes.toBytes(family), 1000); // family is ignore
        assertEquals(delete.toJSON(), target.toJSON());
    }

    @Test
    public void testDeleteWithFamilyVersion() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("pk9"));
        delete.addFamilyVersion(Bytes.toBytes(family), 3000);

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        target.addFamilyVersion(Bytes.toBytes(family), 3000);
        assertEquals(delete.toJSON(), target.toJSON());
    }

    @Test
    public void testDeleteWithColumns() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("pk10"));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes("col-1"));

        ODelete odelete= ElementConvertor.toOtsDelete(delete, tablestoreColumnMapping);
        Delete target = ElementConvertor.toHBaseDelete(odelete, tablestoreColumnMapping);
        assertEquals(delete.toJSON(), target.toJSON());
    }
}

package samples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloWorld {

    private static final byte[] TABLE_NAME = Bytes.toBytes("Hello-Tablestore");
    private static final byte[] ROW_KEY = Bytes.toBytes("row_1");
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("col_1");
    private static final byte[] COLUMN_VALUE = Bytes.toBytes("col_value");

    public static void main(String[] args) {
        helloWorld();
    }

    private static void helloWorld() {
        try  {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

            System.out.println("Create table " + descriptor.getNameAsString());
            admin.createTable(descriptor);

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            System.out.println("Write one row to the table");
            Put put = new Put(ROW_KEY);
            put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, COLUMN_VALUE);
            table.put(put);

            Result getResult = table.get(new Get(ROW_KEY));
            String value = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
            System.out.println("Get a one row by row key");
            System.out.printf("\t%s = %s\n", Bytes.toString(ROW_KEY), value);

            Scan scan = new Scan();

            System.out.println("Scan for all rows:");
            ResultScanner scanner = table.getScanner(scan);
            for (Result row : scanner) {
                byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                System.out.println('\t' + Bytes.toString(valueBytes));
            }

            System.out.println("Delete the table");
            admin.disableTable(table.getName());
            admin.deleteTable(table.getName());

            table.close();
            admin.close();
            connection.close();
        } catch (IOException e) {
            System.err.println("Exception while running HelloTablestore: " + e.toString());
            System.exit(1);
        }
    }
}
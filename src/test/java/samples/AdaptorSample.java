package samples;

import com.alicloud.tablestore.adaptor.client.OResultScanner;
import com.alicloud.tablestore.adaptor.client.OTSAdapter;
import com.alicloud.tablestore.adaptor.client.TablestoreClientConf;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.struct.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AdaptorSample {

  private OTSAdapter otsAdapter = null;
  private static final String endpoint = "";
  private static final String accessId = "";
  private static final String accessKey = "";
  private static final String instanceName = "";
  private static final String tableName = "ots_adaptor";

  public static void main(String[] args) {
    AdaptorSample sample = new AdaptorSample();

    try {
      sample.put();
      sample.putMultiple();

      sample.get();
      sample.getMultiple();
      sample.scan();
      sample.getScanner();

      sample.useFilter();

      sample.delete();
      sample.deleteMultiple();
    } catch (IOException ex) {
      System.out.println("Occur exception:" + ex.getCause());
    } finally {
      sample.close();
    }
  }

  public AdaptorSample() {
    TablestoreClientConf conf = new TablestoreClientConf();
    conf.setOTSEndpoint(endpoint);
    conf.setTablestoreAccessKeyId(accessId);
    conf.setTablestoreAccessKeySecret(accessKey);
    conf.setOTSInstanceName(instanceName);
    otsAdapter = OTSAdapter.getInstance(conf);
  }

  public void put() throws IOException {
    // create put object.
    byte[] rowKey = Bytes.toBytes("row_001");
    OPut put = new OPut(rowKey);

    // add new columnValue. qualifier = "qualifier_001", timestamp = 10000, value = "value_001".
    put.add(Bytes.toBytes("qualifier_001"), 1000, Bytes.toBytes("value_001"));

    // add new columnValue. qualifier = "qualifier_002", value = "value_002", use server timestamp.
    put.add(Bytes.toBytes("qualifier_002"), Bytes.toBytes("value_002"));

    // put
    otsAdapter.put(tableName, put);

    System.out.println("Put row:row_001 succeeded");
  }

  public void putMultiple() throws IOException {

    List<OPut> puts = new ArrayList<OPut>();

    for (int i = 0; i < 10; i++) {
      // create put object.
      byte[] rowKey = Bytes.toBytes("row_" + i);
      OPut put = new OPut(rowKey);

      // add new columnValue. qualifier = "qualifier_001", timestamp = 10000, value = "value_001".
      put.add(Bytes.toBytes("qualifier_001"), 1000, Bytes.toBytes("value_001"));

      // add new columnValue. qualifier = "qualifier_002", value = "value_002", use server timestamp.
      put.add(Bytes.toBytes("qualifier_002"), Bytes.toBytes("value_002"));

      // add put to list
      puts.add(put);
    }

    otsAdapter.putMultiple(tableName, puts);
    System.out.println("Multi put row:row_001 succeeded");
  }

  public void get() throws IOException {

    // create get object
    OGet get = new OGet(Bytes.toBytes("row_001"));

    // setMaxVersions. (optional, default 1).
    get.setMaxVersions(3);

    // setTimeRange. (optional, default [0, Long.maxValue) )
    get.setTimeRange(0, System.currentTimeMillis());

    // add column to get. (optional, default get all columns)
    get.addColumn(Bytes.toBytes("qualifier_001"));

    OResult result = otsAdapter.get(tableName, get);

    List<OColumnValue> values = result.getColumn(Bytes.toBytes("qualifier_001"));

    System.out.println("Get row succeeded");
    for (OColumnValue value : values) {
      System.out.println(value.getRow().toString());
    }
  }

  public void getMultiple() throws IOException {

    List<OGet> gets = new ArrayList<OGet>();

    for (int i = 0; i < 10; i++) {
      // create get object
      OGet get = new OGet(Bytes.toBytes("row_" + i));

      // setMaxVersions. (optional, default 1).
      get.setMaxVersions(3);

      // setTimeRange. (optional, default [0, Long.maxValue) )
      get.setTimeRange(0, System.currentTimeMillis());

      // add column to get. (optional, default get all columns)
      get.addColumn(Bytes.toBytes("qualifier_001"));

      // add get to list
      gets.add(get);
    }

    List<OResult> results = otsAdapter.getMultiple(tableName, gets);
    System.out.println("Multiple get row succeeded");
    for (OResult result : results) {
      System.out.println(result.getRow().toString());
    }
  }

  public void delete() throws IOException {

    // create delete object
    ODelete delete1 = new ODelete(Bytes.toBytes("row_001"));

    // delete row. (rowkey = "row_001")
    otsAdapter.delete(tableName, delete1);

    ODelete delete2 = new ODelete(Bytes.toBytes("row_002"));

    delete2.deleteColumns(Bytes.toBytes("qualifier_001"));

    // delete all versions of qualifier_001.
    otsAdapter.delete(tableName, delete2);

    ODelete delete3 = new ODelete(Bytes.toBytes("row_003"));

    delete3.deleteColumn(Bytes.toBytes("qualifier_001"), 10000);

    // delete one columnValue(qualifier = "qualifier_001, timestamp = 10000)
    otsAdapter.delete(tableName, delete3);

    ODelete delete4 = new ODelete(Bytes.toBytes("row_004"));

    delete4.deleteColumns(Bytes.toBytes("qualifier_001"));
    delete4.deleteColumns(Bytes.toBytes("qualifier_002"));
    delete4.deleteColumn(Bytes.toBytes("qualifier_003"), 10000);

    // delete all versions of qualifier_001,
    // all versions of qualifier_002
    // and one version of qualifier_003
    otsAdapter.delete(tableName, delete4);
    System.out.println("Delete row succeeded.");
  }

  public void deleteMultiple() throws IOException {

    List<ODelete> deletes = new ArrayList<ODelete>();

    // create delete object
    ODelete delete1 = new ODelete(Bytes.toBytes("row_001"));

    // add delete1 to list.
    deletes.add(delete1);

    ODelete delete2 = new ODelete(Bytes.toBytes("row_002"));

    delete2.deleteColumns(Bytes.toBytes("qualifier_001"));

    // add delete2 to list.
    deletes.add(delete2);

    ODelete delete3 = new ODelete(Bytes.toBytes("row_003"));

    delete3.deleteColumn(Bytes.toBytes("qualifier_001"), 10000);

    // add delete3 to list.
    deletes.add(delete3);

    ODelete delete4 = new ODelete(Bytes.toBytes("row_004"));

    delete4.deleteColumns(Bytes.toBytes("qualifier_001"));
    delete4.deleteColumns(Bytes.toBytes("qualifier_002"));
    delete4.deleteColumn(Bytes.toBytes("qualifier_003"), 10000);

    // add delete4 to list.
    deletes.add(delete4);

    otsAdapter.deleteMultiple(tableName, deletes);
    System.out.println("Multiper delete row succeeded.");
  }

  public void scan() throws IOException {

    OScan scan = new OScan();

    // scan all rows. (return rows not up to 100)
    List<OResult> results = otsAdapter.scan(tableName, scan, 100);

    // add some restricted conditions.
    scan.setStartRow(Bytes.toBytes("row_001"));
    scan.setStopRow(Bytes.toBytes("row_010"));
    scan.setMaxVersions(3);
    scan.setTimeRange(0, 10000000);

    // scan rows which meet the conditions. (return rows not up to 100)
    results = otsAdapter.scan(tableName, scan, 100);

    System.out.println("Scan row succeeded.");
    Iterator<OResult>  it = results.iterator();
    while (it.hasNext()) {
      System.out.println(it.next().getRow().toString());
    }
  }

  public void getScanner() throws IOException {

    OScan scan = new OScan();

    // add some restricted conditions.
    scan.setStartRow(Bytes.toBytes("row_001"));
    scan.setStopRow(Bytes.toBytes("row_010"));
    scan.setMaxVersions(3);
    scan.setTimeRange(0, 10000000);

    // get scanner
    OResultScanner resultScanner = otsAdapter.getScanner(tableName, scan);

    // get iterator
    Iterator<OResult> iterator = resultScanner.iterator();

    // iterating to get results.
    System.out.println("GetScan succeeded.");
    while (iterator.hasNext()) {
      OResult result = iterator.next();
      System.out.println(result.getRow().toString());
    }
  }

  public void useFilter() throws IOException {

    OSingleColumnValueFilter filter =
            new OSingleColumnValueFilter(Bytes.toBytes("qualifier_001"),
                    OSingleColumnValueFilter.OCompareOp.EQUAL, Bytes.toBytes("value_001"));

    // optional
    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(false);

    // get with filter
    OGet get = new OGet(Bytes.toBytes("row_001"));
    get.setFilter(filter);
    OResult result = otsAdapter.get(tableName, get);

    // scan with filter
    OScan scan = new OScan();
    scan.setFilter(filter);
    List<OResult> results = otsAdapter.scan(tableName, scan, 100);
    System.out.println("Use filter succeeded.");
  }

  public void close() {
    try {
        otsAdapter.close();
    } catch (IOException ex) {
        System.out.println("occur exception:" + ex.toString());
    }
    System.out.println("Close succeeded.");
  }

}

package com.alicloud.tablestore.adaptor.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.struct.*;

public class PerformanceTest {
  private static String endpoint = "";
  private static String accessId = "";
  private static String accessKey = "";
  private static String instanceName = "";
  private static String tableName = "ots_adaptor";

  public static void main(String[] args) throws InterruptedException, IOException {
    PerformanceTest pt = new PerformanceTest();
    int[] arg = new int[4];
    if (args.length > 0) {
      if (args[0].equals("put")) {
        for (int i = 0; i < 4; i++) {
          arg[i] = Integer.valueOf(args[i + 1]);
        }
        pt.testPutUseOtsSDK(arg[0], arg[1], arg[2], arg[3]);
        pt.testPutUseOtsAdapter(arg[0], arg[1], arg[2], arg[3]);
      }
      if (args[0].equals("get")) {
        for (int i = 0; i < 3; i++) {
          arg[i] = Integer.valueOf(args[i + 1]);
        }
        pt.testGetUseOtsSDK(arg[0], arg[1]);
        pt.testGetUseOtsAdapter(arg[0], arg[1], arg[2]);
      }
      if (args[0].equals("scan")) {
        for (int i = 0; i < 3; i++) {
          arg[i] = Integer.valueOf(args[i + 1]);
        }
        pt.testScanUseOtsSDK(arg[0], arg[1], arg[2]);
        pt.testScanUseOtsAdapter(arg[0], arg[1], arg[2]);
      }
    } else {
      pt.test();
//      pt.testPutUseOtsAdapter(10, 10000, 10, 10);
    }
  }

  private SyncClient getOTSClient() {
    ClientConfiguration conf = new ClientConfiguration();
    conf.setTimeThresholdOfTraceLogger(100);
    SyncClient ots = new SyncClient(endpoint, accessId, accessKey, instanceName, conf);
    return ots;
  }

  private OTSAdapter getOTSAdapter() {
    TablestoreClientConf conf = new TablestoreClientConf();
    conf.setOTSEndpoint(endpoint);
    conf.setTablestoreAccessKeyId(accessId);
    conf.setTablestoreAccessKeySecret(accessKey);
    conf.setOTSInstanceName(instanceName);
    OTSAdapter otsAdapter = OTSAdapter.getInstance(conf);
    return otsAdapter;
  }

  private void createTable() {
    SyncClient ots = getOTSClient();
    try {
      ots.deleteTable(new DeleteTableRequest(tableName));
    } catch (TableStoreException ex) {
      if (!ex.getErrorCode().equals("OTSObjectNotExist")) {
        throw ex;
      }
    }
    PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema("__rowkey__", PrimaryKeyType.BINARY);
    TableMeta tableMeta = new TableMeta(tableName);
    tableMeta.addPrimaryKeyColumn(primaryKeySchema);
    TableOptions options = new TableOptions(30 * 24 * 3600, 1);
    CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, options);
    TableOptions tableOptions = new TableOptions();
    tableOptions.setMaxVersions(3);
    tableOptions.setTimeToLive(Integer.MAX_VALUE);
    createTableRequest.setTableOptions(tableOptions);
    ots.createTable(createTableRequest);
    ots.shutdown();
  }

  private void printInfo(long totalTime, int requestCount, int[] latency) {
    System.out.println("" + totalTime / 1000.0 + "sec");
    long sum = 0;
    long[] counts = new long[8];
    for (int i = 0; i < requestCount; i++) {
      sum += latency[i];
      if (latency[i] < 1) {
        counts[0]++;
        continue;
      }
      if (latency[i] < 5) {
        counts[1]++;
        continue;
      }
      if (latency[i] < 10) {
        counts[2]++;
        continue;
      }
      if (latency[i] < 50) {
        counts[3]++;
        continue;
      }
      if (latency[i] < 100) {
        counts[4]++;
        continue;
      }
      if (latency[i] < 500) {
        counts[5]++;
        continue;
      }
      if (latency[i] < 1000) {
        counts[6]++;
        continue;
      }
      counts[7]++;
    }
    System.out.println("TotalRequest: " + requestCount);
    System.out.println("Qps: " + requestCount * 1000 / (double) totalTime);
    System.out.println("AverLatency: " + sum / (double) requestCount);
    for (int i = 0; i < 8; i++) {
      System.out.print(" " + counts[i] / (double) requestCount);
    }
    System.out.println();
  }

  public void testPutUseOtsSDK(int threadNum, final int rowsPerThread, final int columnNum,
                               final int rowsPerRequest) throws InterruptedException {
    createTable();
    Thread.sleep(5 * 1000);
    final SyncClient ots = getOTSClient();
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * rowsPerThread / rowsPerRequest;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = rowsPerThread;
          BatchWriteRowRequest batch = new BatchWriteRowRequest();
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OPut put = new OPut(Bytes.toBytes("" + id));
            for (int j = 0; j < columnNum; j++) {
              put.add(Bytes.toBytes("column" + j), Bytes.toBytes("value" + id + j));
            }
            batch.addRowChange(put.toOTSParameter(tableName));
            try {
              long time0 = System.currentTimeMillis();
              if (id > finalI * count && (id + 1) % rowsPerRequest == 0) {
                ots.batchWriteRow(batch);
                batch = new BatchWriteRowRequest();
                latency[id / rowsPerRequest] = (int) (System.currentTimeMillis() - time0);
              }
            } catch (Exception e) {
              batch = new BatchWriteRowRequest();
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS SDK:");
    printInfo(totalTime, requestCount, latency);
    ots.shutdown();
  }

  public void testPutUseOtsAdapter(int threadNum, final int rowsPerThread, final int columnNum,
                                   final int rowsPerRequest) throws InterruptedException, IOException {
    createTable();
    Thread.sleep(5 * 1000);
    final OTSAdapter otsAdapter = getOTSAdapter();
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * rowsPerThread / rowsPerRequest;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = rowsPerThread;
          List<OPut> puts = new ArrayList<OPut>();
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OPut put = new OPut(Bytes.toBytes("" + id));
            for (int j = 0; j < columnNum; j++) {
              put.add(Bytes.toBytes("column" + j), Bytes.toBytes("value" + id + j));
            }
            puts.add(put);
            try {
              long time0 = System.currentTimeMillis();
              if (id > finalI * count && (id + 1) % rowsPerRequest == 0) {
                otsAdapter.putMultiple(tableName, puts);
                puts = new ArrayList<OPut>();
                latency[id / rowsPerRequest] = (int) (System.currentTimeMillis() - time0);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS Adapter:");
    printInfo(totalTime, requestCount, latency);
    otsAdapter.close();
  }

  public void testGetUseOtsSDK(int threadNum, final int rowsPerThread) throws InterruptedException {
    final SyncClient ots = getOTSClient();
    System.out.println("Please ensure that the data to get is ready.");
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * rowsPerThread;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = rowsPerThread;
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OGet get = new OGet(Bytes.toBytes("" + id));
            try {
              long time0 = System.currentTimeMillis();
              ots.getRow(new GetRowRequest(get.toOTSParameter(tableName)));
              latency[id] = (int) (System.currentTimeMillis() - time0);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS SDK:");
    printInfo(totalTime, requestCount, latency);
    ots.shutdown();
  }

  public void
  testGetUseOtsAdapter(int threadNum, final int rowsPerThread, final int rowsPerRequest)
          throws InterruptedException, IOException {
    System.out.println("Please ensure that the data to get is ready.");
    final OTSAdapter otsAdapter = getOTSAdapter();
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * rowsPerThread / rowsPerRequest;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = rowsPerThread;
          List<OGet> gets = new ArrayList<OGet>();
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OGet get = new OGet(Bytes.toBytes("" + id));
            gets.add(get);
            try {
              long time0 = System.currentTimeMillis();
              if (id > finalI * count && (id + 1) % rowsPerRequest == 0) {
                otsAdapter.getMultiple(tableName, gets);
                gets = new ArrayList<OGet>();
                latency[id / rowsPerRequest] = (int) (System.currentTimeMillis() - time0);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS Adapter:");
    printInfo(totalTime, requestCount, latency);
    otsAdapter.close();
  }

  public void testScanUseOtsSDK(int threadNum, final int scansPerThread, final int rowsPerScan)
          throws InterruptedException {
    System.out.println("Please ensure that the data to scan is ready.");
    final SyncClient ots = getOTSClient();
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * scansPerThread;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = scansPerThread;
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OScan scan = new OScan(Bytes.toBytes("" + id));
            scan.setCaching(rowsPerScan);
            try {
              long time0 = System.currentTimeMillis();
              ots.getRange(new GetRangeRequest(scan.toOTSParameter(tableName)));
              latency[id] = (int) (System.currentTimeMillis() - time0);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS SDK:");
    printInfo(totalTime, requestCount, latency);
    ots.shutdown();
  }

  public void testScanUseOtsAdapter(int threadNum, final int scansPerThread, final int rowsPerScan)
          throws InterruptedException, IOException {
    System.out.println("Please ensure that the data to scan is ready.");
    final OTSAdapter otsAdapter = getOTSAdapter();
    List<Thread> threads = new ArrayList<Thread>();
    int requestCount = threadNum * scansPerThread;
    final int[] latency = new int[requestCount];
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      threads.add(new Thread(new Runnable() {
        public void run() {
          int count = scansPerThread;
          for (int id = finalI * count; id < finalI * count + count; id++) {
            OScan scan = new OScan(Bytes.toBytes("" + id));
            scan.setCaching(rowsPerScan);
            try {
              long time0 = System.currentTimeMillis();
              otsAdapter.scan(tableName, scan, rowsPerScan);
              latency[id] = (int) (System.currentTimeMillis() - time0);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }));
    }
    long time0 = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long totalTime = System.currentTimeMillis() - time0;
    System.out.println("Use OTS Adapter:");
    printInfo(totalTime, requestCount, latency);
    otsAdapter.close();
  }

  public void test() throws InterruptedException, IOException {
    createTable();
    Thread.sleep(5 * 1000);
    OTSAdapter otsAdapter = getOTSAdapter();
    otsAdapter.setAutoFlush(tableName, false);
    otsAdapter.setWriteBufferSize(tableName, 2000 * 1000);
    int count = 100000;
    long time0 = System.currentTimeMillis();
    byte[][] rows = new byte[count][];
    List<OGet> gets = new ArrayList<OGet>();
    for (int i = 0; i < count; i++) {
      OPut put = new OPut(Bytes.toBytes("" + i));
      OGet get = new OGet(Bytes.toBytes("" + i));
      gets.add(get);
      for (int j = 0; j < 10; j++) {
        put.add(Bytes.toBytes("column" + j), Bytes.toBytes("value" + i + j));
      }
      rows[i] = Bytes.toBytes("" + i);
      otsAdapter.put(tableName, put);
    }
    otsAdapter.flushCommits(tableName);
    Arrays.sort(rows, Bytes.BYTES_COMPARATOR);
    OScan scan = new OScan();
    OResultScanner scanner = otsAdapter.getScanner(tableName, scan);
    Iterator<OResult> iterator = scanner.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      OResult result = iterator.next();
      assertTrue(Bytes.equals(rows[i], result.getRow()));
      for (int j = 0; j < 10; j++) {
        assertEquals("value" + Bytes.toString(rows[i]) + j,
                Bytes.toString(result.getValue(Bytes.toBytes("column" + j))));
      }
      i++;
    }
    assertEquals(count, i);
    List<OResult> results = otsAdapter.getMultiple(tableName, gets);
    for (i = 0; i < count; i++) {
      for (int j = 0; j < 10; j++) {
        assertEquals("value" + i + j,
                Bytes.toString(results.get(i).getValue(Bytes.toBytes("column" + j))));
      }
    }
    List<ODelete> deletes = new ArrayList<ODelete>();
    for (i = 0; i < count; i++) {
      deletes.add(new ODelete(rows[i]));
    }
    otsAdapter.deleteMultiple(tableName, deletes);
    results = otsAdapter.scan(tableName, scan, 100);
    assertEquals(0, results.size());
    scanner = otsAdapter.getScanner(tableName, scan);
    iterator = scanner.iterator();
    assertEquals(false, iterator.hasNext());

    otsAdapter.close();
  }
}

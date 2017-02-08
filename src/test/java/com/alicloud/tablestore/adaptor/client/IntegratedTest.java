/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter.OCompareOp;
import com.alicloud.tablestore.adaptor.struct.*;

public class IntegratedTest {
  public static final Log LOG = LogFactory.getLog(IntegratedTest.class);
  private static ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static final String KEY_SEED = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final int KEY_SEED_LEN = KEY_SEED.length();
  private static final char[] KEY_SEED_CHARS = KEY_SEED.toCharArray();

  public static final byte[] QUAL_NAME = Bytes.toBytes("q1");
  public static final int ROW_LEN = 50;
  public static final int VAL_LEN = 100;
  private final OTSAdapter service;

  private static String endpoint = "";
  private static String accessId = "";
  private static String accessKey = "";
  private static String instanceName = "";
  private static String tableName = "ots_adaptor";

  public static byte[] generateBytes(int length) {
    StringBuilder keyBuilder = new StringBuilder(length);
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      keyBuilder.append(KEY_SEED_CHARS[random.nextInt(KEY_SEED_LEN)]);
    }
    return Bytes.toBytes(keyBuilder.toString());
  }

  public IntegratedTest() {
    TablestoreClientConf conf = new TablestoreClientConf();
    conf.setOTSEndpoint(endpoint);
    conf.setTablestoreAccessKeyId(accessId);
    conf.setTablestoreAccessKeySecret(accessKey);
    conf.setOTSInstanceName(instanceName);
    conf.setOperationTimeout(30 * 1000);
    service = OTSAdapter.getInstance(conf);
  }

  public void close() throws IOException {
    service.close();
  }

  public IntegratedTest(TablestoreClientConf conf) {
    service = OTSAdapter.getInstance(conf);
  }

  public int doMultiGet(String tableName, List<byte[]> rows, int batchCount) throws Exception {
    int count = 0;
    for (int i = 0; i < rows.size(); i += batchCount) {
      List<OGet> getList = new ArrayList<OGet>();
      for (int k = 0; k < batchCount && (k + i < rows.size()); k++) {
        OGet get = new OGet(rows.get(k + i));
        getList.add(get);
      }
      List<OResult> results = service.getMultiple(tableName, getList);
      for (OResult r : results) {
        if (r != null && !r.isEmpty()) {
          count++;
        }
      }
    }
    return count;
  }

  public int doGet(String tableName, List<byte[]> rows) throws Exception {
    int count = 0;
    for (byte[] row : rows) {
      OGet get = new OGet(row);
      OResult r = service.get(tableName, get);
      if (r != null && !r.isEmpty()) {
        count++;
      }
    }
    return count;
  }

  public void doDelete(String tableName, List<byte[]> rows) throws Exception {
    for (byte[] row : rows) {
      ODelete delete = new ODelete(row);
      service.delete(tableName, delete);
    }
  }

  public void doScanWithFilter(String tableName) throws Exception {
    OScan scan = new OScan();
    OSingleColumnValueFilter filter =
        new OSingleColumnValueFilter(Bytes.toBytes("NONEXISTQUALIFIER"), OCompareOp.EQUAL,
            Bytes.toBytes("NONEXISTVALUE"));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    OResultScanner scanner = service.getScanner(tableName, scan);
    int count = 0;
    for (OResult r : scanner) {
      count++;
    }
    if (count > 0) {
      throw new RuntimeException("Except count=0,Actual=" + count);
    }
  }

  // public void doScanWithFilter2(String tableName) throws Exception {
  // // 构造查询的Scan,确定Row的范围
  // HScan scan = new HScan(Bytes.toBytes("aaa"), Bytes.toBytes("ggg"));
  // // 构造一个列值过滤器:列f1:q1的值='value1'
  // SingleColumnValueFilterStringHelper filter = new SingleColumnValueFilterStringHelper(
  // Bytes.toBytes("f1"), Bytes.toBytes("q1"), CompareOp.EQUAL, Bytes.toBytes("value1"));
  // //为scan设置过滤器
  // scan.setFilter(filter.toParseableString());
  // HResultScanner scanner = HBaseCloudService.getScanner(tableName, scan);
  // for (HResult r : scanner) {
  // // 扫描得到的每一行数据中列f1:q1的值='value1'
  // System.out.println(r);
  // }
  // scanner.close();
  // }

  public int count(String tableName) throws Exception {
    OScan scan = new OScan();
    OResultScanner scanner = service.getScanner(tableName, scan);
    int count = 0;
    for (OResult r : scanner) {
      count++;
    }
    return count;
  }

  public int doScanner(String tableName, int num) throws Exception {
    OScan scan = new OScan();
    scan.setStartRow(generateBytes(ROW_LEN));
    OResultScanner scanner = service.getScanner(tableName, scan);
    int count = 0;
    for (int i = 0; i < num && scanner.next() != null; i++) {
      count++;
    }
    return count;
  }

  public void doMultiPut(String tableName, int num, int batchCount) throws IOException {
    for (int i = 0; i < num; i += batchCount) {
      List<OPut> putList = new ArrayList<OPut>();
      for (int k = 0; k < batchCount; k++) {
        OPut put = new OPut(generateBytes(ROW_LEN));
        put.add(QUAL_NAME, generateBytes(VAL_LEN));
        putList.add(put);
      }
      service.putMultiple(tableName, putList);
    }
  }

  public void doPut(String tableName, int num) throws IOException {
    for (int i = 0; i < num; i++) {
      OPut put = new OPut(generateBytes(ROW_LEN));
      put.add(QUAL_NAME, generateBytes(VAL_LEN));
      service.put(tableName, put);
    }
  }

  public void doNonAutoFlushPut(String tableName, List<byte[]> rows) throws IOException {
    service.setAutoFlush(tableName, false);
    try {
      for (byte[] row : rows) {
        OPut put = new OPut(row);
        put.add(QUAL_NAME, generateBytes(VAL_LEN));
        service.put(tableName, put);
      }
    } finally {
      service.setAutoFlush(tableName, true);
    }
  }

  public void doPutInMultiThread(final String tableName, final int num, int threadNum)
      throws IOException, InterruptedException {
    Thread[] threads = new Thread[threadNum];
    for (int i = 0; i < threadNum; i++) {
      threads[i] = new Thread() {
        public void run() {
          try {
            doPut(tableName, num);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      threads[i].start();
    }
    for (int i = 0; i < threadNum; i++) {
      threads[i].join();
    }
  }

  public void doPut(String tableName, List<byte[]> rows) throws IOException {
    for (byte[] row : rows) {
      OPut put = new OPut(row);
      put.add(QUAL_NAME, generateBytes(VAL_LEN));
      service.put(tableName, put);
    }
  }

  public void doBatch(String tableName, List<byte[]> getRows, List<byte[]> putRows)
      throws IOException {
    List<ORow> actions = new ArrayList<ORow>();
    for (byte[] row : getRows) {
      OGet get = new OGet(row);
      actions.add(get);
    }

    for (byte[] row : putRows) {
      OPut put = new OPut(row);
      put.add(QUAL_NAME, generateBytes(VAL_LEN));
      actions.add(put);
    }

//    // Get bad_family
//    OGet badGet = new OGet(Bytes.toBytes("bad_row"));
//    actions.add(badGet);

    Object[] results = new Object[actions.size()];

    service.batch(tableName, actions, results);

    for (int i = 0; i < getRows.size(); i++) {
      try {
        OResult r = (OResult) results[i];
        if (!(r != null && !r.isEmpty())) {
          throw new RuntimeException("Unexpected " + i + " result " + r + " for row "
                  + Bytes.toString(getRows.get(i)));
        }
      } catch (Exception ex) {
        System.out.println(ex);
        System.out.println(results[i]);
      }
    }
    for (int i = 0; i < putRows.size(); i++) {
      try {
        OResult r = (OResult) results[i + getRows.size()];
        if (!(r != null && r.size() == 0)) {
          throw new RuntimeException("Unexpected " + i + " result " + r + " for row "
                  + Bytes.toString(putRows.get(i)));
        }
      } catch (Exception ex) {
        System.out.println(ex);
        System.out.println(results[i + getRows.size()]);
      }

    }

//    if (!(results[results.length - 1] instanceof Throwable)) {
//      throw new RuntimeException("Expecting return error but return  "
//          + results[results.length - 1]);
//    }
  }

  private void createTable() {
    SyncClient ots = new SyncClient(endpoint, accessId, accessKey, instanceName);
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
    TableOptions options = new TableOptions(30 * 24 * 2600, 1);
    CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, options);
    TableOptions tableOptions = new TableOptions();
    tableOptions.setMaxVersions(3);
    tableOptions.setTimeToLive(Integer.MAX_VALUE);
    createTableRequest.setTableOptions(tableOptions);
    ots.createTable(createTableRequest);
    ots.shutdown();
  }

  public void doSimpleTest() throws Exception {
    createTable();
    Thread.sleep(10 * 1000);

    System.out.println("Testing put in multi threads...");
    this.doPutInMultiThread(tableName, 1000, 10);
    System.out.println("Testing put...");
    this.doPut(tableName, 1000);
    System.out.println("Testing multiPut...");
    this.doMultiPut(tableName, 1000, 10);
    int count = this.count(tableName);
    if (count != 12000) {
      throw new RuntimeException("Except count=12000,Actual=" + count);
    }

    doScanWithFilter(tableName);

    List<byte[]> rows = new ArrayList<byte[]>();
    for (int i = 0; i < 500; i++) {
      rows.add(generateBytes(ROW_LEN));
    }
    this.doPut(tableName, rows);
    System.out.println("Testing get...");
    count = this.doGet(tableName, rows);
    if (count != 500) {
      throw new RuntimeException("Except count=500,Actual=" + count);
    }
    System.out.println("Testing multiGet...");
    count = this.doMultiGet(tableName, rows, 10);
    if (count != 500) {
      throw new RuntimeException("Except count=500,Actual=" + count);
    }

    System.out.println("Testing batch...");
    List<byte[]> newRows = new ArrayList<byte[]>();
    for (int i = 0; i < 500; i++) {
      newRows.add(generateBytes(ROW_LEN));
    }
    doBatch(tableName, rows, newRows);

    System.out.println("Testing delete...");
    doDelete(tableName, rows);
    count = this.doGet(tableName, rows);
    if (count != 0) {
      throw new RuntimeException("Except count=0,Actual=" + count);
    }

    System.out.println("Testing non-autoflush...");
    newRows = new ArrayList<byte[]>();
    for (int i = 0; i < 5; i++) {
      newRows.add(generateBytes(ROW_LEN));
    }
    this.doNonAutoFlushPut(tableName, newRows);
    count = this.doGet(tableName, newRows);
    if (count != 0) {
      throw new RuntimeException("Except count=0,Actual=" + count);
    }
    this.service.flushCommits(tableName);
    count = this.doGet(tableName, newRows);
    if (count != 5) {
      throw new RuntimeException("Except count=5,Actual=" + count);
    }

    System.out.println("Pass the simple test!");
  }

  public static void printThreadInfo() {
    String title = "Automatic Stack Trace";
    PrintWriter stream = new PrintWriter(System.out);
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, 20);
      if (info == null) {
        stream.println("  Inactive");
      } else {
        stream.println("Thread " + getTaskName(info.getThreadId(), info.getThreadName()) + ":");

        Thread.State state = info.getThreadState();
        stream.println("  State: " + state);
        stream.println("  Blocked count: " + info.getBlockedCount());
        stream.println("  Waited count: " + info.getWaitedCount());
        if (contention) {
          stream.println("  Blocked time: " + info.getBlockedTime());
          stream.println("  Waited time: " + info.getWaitedTime());
        }
        if (state == Thread.State.WAITING) {
          stream.println("  Waiting on " + info.getLockName());
        } else if (state == Thread.State.BLOCKED) {
          stream.println("  Blocked on " + info.getLockName());
          stream.println("  Blocked by "
              + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
        }

        stream.println("  Stack:");
        for (StackTraceElement frame : info.getStackTrace())
          stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public static void main(String[] args) throws Exception {
    final IntegratedTest it1 = new IntegratedTest();
    it1.doSimpleTest();
    it1.close();

  }
}

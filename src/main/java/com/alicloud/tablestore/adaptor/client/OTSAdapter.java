package com.alicloud.tablestore.adaptor.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.struct.OTableDescriptor;

/**
 * A fault-tolerant and integrated OTSInterface implementation which balances the request to
 * multiple OTS servers. Application could use this class directly to acesss OTS Data Service. A
 * Simple Example: TablestoreClientConf conf = new TablestoreClientConf(); OTSAdapter adapter =
 * OTSAdapter.getInstance(conf); OGet get = new OGet(Bytes.toBytes("11111")); OResult result =
 * adapter.get("testTable", get); adapter.close();
 */
public class OTSAdapter implements OTSInterface {
  /**
   * table attribute constants
   */
  private static final String AUTO_FLUSH = "_autoflush_";
  private static final String CLEAR_BUFFER_ON_FAIL = "_clearbufferonfail_";
  private static final String WRITE_BUFFER_SIZE = "_writebuffersize_";

  // A Map of TablestoreClientConf -> OTSAdapter (HCS). All access must be
  // synchronized. This map is not private because tests need to be able to
  // tinker with it.
  static final Map<TablestoreClientConf, OTSAdapter> OTS_INSTANCES =
      new HashMap<TablestoreClientConf, OTSAdapter>();
  // Used for test
  static boolean mockMode = false;

  private final TablestoreClientConf clientConf;
  // A Proxy to access adaptor service
  private OTSInterface otsProxy;
  int refCount;

  private static final String SEPARATOR = ".";
  // A map of table attributes. The map key is composed of Table_Name +
  // SEPARATOR + Attribute_Name, and the value is byte value of attribute.
  private ConcurrentHashMap<String, byte[]> tableAttributes;

  private final ConcurrentHashMap<String, ArrayList<com.alicloud.tablestore.adaptor.struct.OPut>> tableWriteBuffers =
      new ConcurrentHashMap<String, ArrayList<com.alicloud.tablestore.adaptor.struct.OPut>>();
  private final ConcurrentHashMap<String, AtomicLong> tableCurrentBufferSizes =
      new ConcurrentHashMap<String, AtomicLong>();

  private OTSAdapter(TablestoreClientConf conf) {
    this.clientConf = conf;
    initAndStart();
  }

  private void initAndStart() {
    if (mockMode) return;
    otsProxy = (OTSInterface) com.alicloud.tablestore.adaptor.client.RetryProxy.create(this.clientConf);
  }

  /**
   * Get the instance of OTSAdapter by the specified config.
   * @param conf
   * @return the unique global instance
   */
  public static synchronized OTSAdapter getInstance(TablestoreClientConf conf) {
    validateConf(conf);
    synchronized (OTS_INSTANCES) {
      OTSAdapter hbaseCloudService = OTS_INSTANCES.get(conf);
      if (hbaseCloudService == null) {
        hbaseCloudService = new OTSAdapter(conf);
        OTS_INSTANCES.put(conf, hbaseCloudService);
      }
      hbaseCloudService.incCount();
      return hbaseCloudService;
    }
  }

  @Override
  public com.alicloud.tablestore.adaptor.struct.OResult get(String tableName, com.alicloud.tablestore.adaptor.struct.OGet get) throws IOException {
    return otsProxy.get(tableName, get);
  }

  @Override
  public void put(String tableName, com.alicloud.tablestore.adaptor.struct.OPut put) throws IOException {
    if (getAutoFlush(tableName)) {
      otsProxy.put(tableName, put);
    } else {
      doPut(tableName, Arrays.asList(put));
    }

  }

  @Override
  public void putMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OPut> puts) throws IOException {
    if (getAutoFlush(tableName)) {
      otsProxy.putMultiple(tableName, puts);
    } else {
      doPut(tableName, puts);
    }
  }

  private void doPut(String tableName, final List<com.alicloud.tablestore.adaptor.struct.OPut> puts) throws IOException {
    ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> tableWriteBuffer = getTableWriteBuffer(tableName);
    AtomicLong currentBufferSize = getTableCurrentBufferSize(tableName);
    boolean autoFlush = getAutoFlush(tableName);
    long writeBufferSize = getWriteBufferSize(tableName);

    List<com.alicloud.tablestore.adaptor.struct.OPut> flushPuts = null;
    synchronized (tableWriteBuffer) {
      for (com.alicloud.tablestore.adaptor.struct.OPut put : puts) {
        tableWriteBuffer.add(put);
        currentBufferSize.addAndGet(put.getWritableSize());
      }
      if (autoFlush || currentBufferSize.get() > writeBufferSize) {
        flushPuts = new ArrayList<com.alicloud.tablestore.adaptor.struct.OPut>(tableWriteBuffer);
        tableWriteBuffer.clear();
        currentBufferSize.set(0);
      }
    }
    if (flushPuts != null && !flushPuts.isEmpty()) {
      doCommits(tableName, flushPuts);
    }
  }

  public void update(String tableName, com.alicloud.tablestore.adaptor.struct.OUpdate update) throws IOException {
    otsProxy.update(tableName, update);
  }

  private ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> getTableWriteBuffer(String tableName) {
    ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> tableWriteBuffer = this.tableWriteBuffers.get(tableName);
    if (tableWriteBuffer == null) {
      tableWriteBuffer = new ArrayList<com.alicloud.tablestore.adaptor.struct.OPut>();
      ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> existedOne = this.tableWriteBuffers.putIfAbsent(tableName, tableWriteBuffer);
      if (existedOne != null) {
        tableWriteBuffer = existedOne;
      }
    }
    return tableWriteBuffer;
  }

  private AtomicLong getTableCurrentBufferSize(String tableName) {
    AtomicLong tableCurrentBufferSize = this.tableCurrentBufferSizes.get(tableName);
    if (tableCurrentBufferSize == null) {
      tableCurrentBufferSize = new AtomicLong();
      AtomicLong existedOne =
          this.tableCurrentBufferSizes.putIfAbsent(tableName, tableCurrentBufferSize);
      if (existedOne != null) {
        tableCurrentBufferSize = existedOne;
      }
    }
    return tableCurrentBufferSize;
  }

  private void doCommits(String tableName, final List<com.alicloud.tablestore.adaptor.struct.OPut> puts) throws IOException {
    boolean flushSuccessfully = false;
    try {
      otsProxy.putMultiple(tableName, puts);
      flushSuccessfully = true;
    } finally {
      if (!flushSuccessfully && !getClearBufferOnFail(tableName)) {
        ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> tableWriteBuffer = getTableWriteBuffer(tableName);
        synchronized (tableWriteBuffer) {
          AtomicLong currentBufferSize = getTableCurrentBufferSize(tableName);
          for (com.alicloud.tablestore.adaptor.struct.OPut put : puts) {
            tableWriteBuffer.add(put);
            currentBufferSize.addAndGet(put.getWritableSize());
          }
        }
      }
    }
  }

  public void flushCommits(String tableName) throws IOException {
    List<com.alicloud.tablestore.adaptor.struct.OPut> flushPuts = null;
    ArrayList<com.alicloud.tablestore.adaptor.struct.OPut> tableWriteBuffer = getTableWriteBuffer(tableName);
    AtomicLong currentBufferSize = getTableCurrentBufferSize(tableName);
    synchronized (tableWriteBuffer) {
      flushPuts = new ArrayList<com.alicloud.tablestore.adaptor.struct.OPut>(tableWriteBuffer);
      tableWriteBuffer.clear();
      currentBufferSize.set(0);
    }
    if (flushPuts != null && !flushPuts.isEmpty()) {
      doCommits(tableName, flushPuts);
    }
  }

  @Override
  public List<com.alicloud.tablestore.adaptor.struct.OResult> getMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OGet> gets) throws IOException {
    return otsProxy.getMultiple(tableName, gets);
  }

  @Override
  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit) throws IOException {
    return otsProxy.scan(tableName, scan, limit);
  }

  @Override
  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit, ByteArrayOutputStream nextRow) throws IOException {
    return otsProxy.scan(tableName, scan, limit, nextRow);
  }

  @Override
  public void delete(String tableName, com.alicloud.tablestore.adaptor.struct.ODelete delete) throws IOException {
    otsProxy.delete(tableName, delete);
  }

  @Override
  public void deleteMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.ODelete> deletes) throws IOException {
    otsProxy.deleteMultiple(tableName, deletes);

  }
  
  @Override
  public void batch(String tableName, List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions, Object[] results)
      throws IOException {
    otsProxy.batch(tableName, actions, results);

  }

  @Override
  public Object[] batch(String tableName, List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions) throws IOException {
    return otsProxy.batch(tableName, actions);
  }
  
  public void setOperationTimeout(int operationTimeout) {
    clientConf.setOperationTimeout(operationTimeout);
  }
  
  public int getOperationTimeout() { 
    return clientConf.getOperationTimeout();
  }

  public void flushAllTableCommits() throws IOException {
    for (String table : tableWriteBuffers.keySet()) {
      flushCommits(table);
    }
  }

  @Override
  public void close() throws IOException {
    flushAllTableCommits();
    synchronized (OTS_INSTANCES) {
      this.decCount();
      if (isZeroReference()) {
        OTS_INSTANCES.remove(this.clientConf);
        if (this.otsProxy != null) {
          com.alicloud.tablestore.adaptor.client.RetryProxy.stopProxy(this.otsProxy);
        }
      }
    }
  }

  /**
   * Returns a scanner on the specified table as specified by the {@link com.alicloud.tablestore.adaptor.struct.OScan} object. Note that
   * the passed {@link com.alicloud.tablestore.adaptor.struct.OScan}'s start row and caching properties maybe changed.
   * @param tableName the table to scan
   * @param scan A configured {@link com.alicloud.tablestore.adaptor.struct.OScan} object.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   */
  public com.alicloud.tablestore.adaptor.client.OResultScanner getScanner(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan) throws IOException {
    return new com.alicloud.tablestore.adaptor.client.OClientScanner(tableName, scan, this.otsProxy);
  }

  /**
   * Increment this client's reference count.
   */
  void incCount() {
    ++refCount;
  }

  /**
   * Decrement this client's reference count.
   */
  void decCount() {
    if (refCount > 0) {
      --refCount;
    }
  }

  /**
   * Return if this client has no reference
   * @return true if this client has no reference; false otherwise
   */
  boolean isZeroReference() {
    return refCount == 0;
  }

  /**
   * Check whether the basic settings are contained in specified conf
   * @param conf
   * @throws IllegalArgumentException
   */
  public static void validateConf(TablestoreClientConf conf) throws IllegalArgumentException {
  }

  /**
   * See {@link #setAutoFlush(String, boolean, boolean)}
   * @param autoFlush Whether or not to enable 'auto-flush'.
   */
  public void setAutoFlush(final String tableName, boolean autoFlush) {
    setAutoFlush(tableName, autoFlush, autoFlush);
  }

  /**
   * Sets the autoFlush flag for the specified tables.
   * @param tableName
   * @param autoFlush Whether or not to enable 'auto-flush'.
   * @param clearBufferOnFail Whether to keep HPut failures in the writeBuffer
   */
  public void setAutoFlush(final String tableName, boolean autoFlush, boolean clearBufferOnFail) {
    setTableAttribute(tableName, AUTO_FLUSH, Bytes.toBytes(autoFlush));
    setTableAttribute(tableName, CLEAR_BUFFER_ON_FAIL, Bytes.toBytes(clearBufferOnFail));
  }

  /**
   * Gets the autoFlush flag for the specified tables.
   * @param tableName
   * @return true if enable 'auto-flush' for specified table
   */
  public boolean getAutoFlush(String tableName) {
    byte[] attr = getTableAttribute(tableName, AUTO_FLUSH);
    return attr == null ? true : Bytes.toBoolean(attr);
  }

  /**
   * Gets the flag of ClearBufferOnFail for the specified tables.
   * @param tableName
   * @return true if keep HPut failures in the writeBuffer
   */
  public boolean getClearBufferOnFail(String tableName) {
    byte[] attr = getTableAttribute(tableName, CLEAR_BUFFER_ON_FAIL);
    return attr == null ? true : Bytes.toBoolean(attr);
  }

  /**
   * Sets the size of the buffer in bytes for the specified tables.
   * <p>
   * @param tableName
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(final String tableName, long writeBufferSize) {
    setTableAttribute(tableName, WRITE_BUFFER_SIZE, Bytes.toBytes(writeBufferSize));
  }

  /**
   * Get the maximum size in bytes of the write buffer for the specified tables.
   * @param tableName
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize(String tableName) {
    byte[] attr = getTableAttribute(tableName, WRITE_BUFFER_SIZE);
    return attr == null ? 2 * 1024 * 1024L : Bytes.toLong(attr);
  }

  public List<String> listTable() throws IOException {
    return otsProxy.listTable();
  }

  public void createTable(OTableDescriptor descriptor) throws IOException {
    otsProxy.createTable(descriptor);
  }

  public void deleteTable(String tableName) throws IOException {
    otsProxy.deleteTable(tableName);
  }

  public OTableDescriptor describeTable(String tableName) throws IOException {
    return otsProxy.describeTable(tableName);
  }

  public void updateTable(OTableDescriptor descriptor) throws IOException {
    otsProxy.updateTable(descriptor);
  }

  private void setTableAttribute(String tableName, String attributeName, byte[] value) {
    if (tableAttributes == null && value == null) {
      return;
    }
    if (tableAttributes == null) {
      tableAttributes = new ConcurrentHashMap<String, byte[]>();
    }
    String name = tableName + SEPARATOR + attributeName;
    if (value == null) {
      tableAttributes.remove(name);
      if (tableAttributes.isEmpty()) {
        this.tableAttributes = null;
      }
    } else {
      tableAttributes.put(name, value);
    }
  }

  private byte[] getTableAttribute(String tableName, String attributeName) {
    if (tableAttributes == null) {
      return null;
    }
    String name = tableName + SEPARATOR + attributeName;
    return tableAttributes.get(name);
  }

}

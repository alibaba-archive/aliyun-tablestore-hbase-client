package com.alicloud.tablestore.adaptor.client;

import java.util.Properties;

/**
 * Provides setting and accessing configuration parameters about ots-client.
 */
public class TablestoreClientConf {

  public static final String TABLESTORE_CLIENT_ENDPOINT = "tablestore.client.endpoint";
  public static final String TABLESTORE_CLIENT_ACCESSID = "tablestore.client.accesskeyid";
  public static final String TABLESTORE_CLIENT_ACCESSKEY = "tablestore.client.accesskeysecret";
  public static final String TABLESTORE_CLIENT_INSTANCENAME = "tablestore.client.instancename";
  public static final String TABLESTORE_CLIENT_MAX_CONNECTIONS = "tablestore.client.max.connections";
  public static final int DEFAULT_TABLESTORE_CLIENT_MAX_CONNECTIONS = 300;
  public static final String TABLESTORE_CLIENT_SOCKET_TIMEOUT = "tablestore.client.socket.timeout";
  public static final int DEFAULT_TABLESTORE_CLIENT_SOCKET_TIMEOUT = 15 * 1000;
  public static final String TABLESTORE_CLIENT_CONNECTION_TIMEOUT = "tablestore.client.connection.timeout";
  public static final int DEFAULT_TABLESTORE_CLIENT_CONNECTION_TIMEOUT = 15 * 1000;
  public static final String TABLESTORE_CLIENT_OPERATION_TIMEOUT = "tablestore.client.operation.timeout";
  public static final int DEFAULT_TABLESTORE_CLIENT_OPERATION_TIMEOUT = Integer.MAX_VALUE;
  public static final String TABLESTORE_CLIENT_RETRIES = "tablestore.client.retries";
  public static final int DEFAULT_TABLESTORE_CLIENT_RETRIES = 3;
  public static final String TABLESTORE_MAX_BATCH_ROW_COUNT = "tablestore.max.batch.row.count";
  public static final int DEFAULT_TABLESTORE_MAX_BATCH_ROW_COUNT = 100;
  public static final String TABLESTORE_MAX_BATCH_DATA_SIZE = "tablestore.max.batch.data.size";
  public static final int DEFAULT_TABLESTORE_MAX_BATCH_DATA_SIZE = 1024 * 1024;
  public static final String TABLESTORE_MAX_SCAN_LIMIT = "tablestore.max.scan.limit";
  public static final int DEFAULT_TABLESTORE_MAX_SCAN_LIMIT = 5000;

  private Properties properties = new Properties();

  /**
   * Get ots endpoint
   * @return the endpoint
   */
  public String getOTSEndpoint() {
    return getValue(TABLESTORE_CLIENT_ENDPOINT);
  }

  /**
   * Set ots endpoint
   * @param endpoint
   */
  public void setOTSEndpoint(String endpoint) {
    setValue(TABLESTORE_CLIENT_ENDPOINT, endpoint);
  }

  /**
   * Get ots accessKeyId
   * @return accessKeyId
   */
  public String getTablestoreAccessKeyId() {
    return getValue(TABLESTORE_CLIENT_ACCESSID);
  }

  /**
   * Set ots accessKeyId
   * @param accessKeyId
   */
  public void setTablestoreAccessKeyId(String accessKeyId) {
    setValue(TABLESTORE_CLIENT_ACCESSID, accessKeyId);
  }

  /**
   * Get ots accessKeySecret
   * @return accessKeySecret
   */
  public String getTablestoreAccessKeySecret() {
    return getValue(TABLESTORE_CLIENT_ACCESSKEY);
  }

  /**
   * Set ots accessKey
   * @param accessKeySecret
   */
  public void setTablestoreAccessKeySecret(String accessKeySecret) {
    setValue(TABLESTORE_CLIENT_ACCESSKEY, accessKeySecret);
  }

  /**
   * Get ots instanceName
   * @return instanceName
   */
  public String getOTSInstanceName() {
    return getValue(TABLESTORE_CLIENT_INSTANCENAME);
  }

  /**
   * Set ots instanceName
   * @param instanceName
   */
  public void setOTSInstanceName(String instanceName) {
    setValue(TABLESTORE_CLIENT_INSTANCENAME, instanceName);
  }

  /**
   * Get max connections of ots client
   * @return max connections
   */
  public int getOTSMaxConnections() {
    return getInt(TABLESTORE_CLIENT_MAX_CONNECTIONS, DEFAULT_TABLESTORE_CLIENT_MAX_CONNECTIONS);
  }

  /**
   * Set ots max connections
   * @param maxConnections
   */
  public void setOTSMaxConnections(int maxConnections) {
    setInt(TABLESTORE_CLIENT_MAX_CONNECTIONS, maxConnections);
  }

  /**
   * Get socket timeout of ots client
   * @return socket timeout
   */
  public int getOTSSocketTimeout() {
    return getInt(TABLESTORE_CLIENT_SOCKET_TIMEOUT, DEFAULT_TABLESTORE_CLIENT_SOCKET_TIMEOUT);
  }

  /**
   * Set socket timeout of ots client
   * @param socketTimeout
   */
  public void setOTSSocketTimeout(int socketTimeout) {
    setInt(TABLESTORE_CLIENT_SOCKET_TIMEOUT, DEFAULT_TABLESTORE_CLIENT_SOCKET_TIMEOUT);
  }

  /**
   * Get connection timeout of ots client
   * @return connection timeout
   */
  public int getOTSConnectionTimeout() {
    return getInt(TABLESTORE_CLIENT_CONNECTION_TIMEOUT, DEFAULT_TABLESTORE_CLIENT_CONNECTION_TIMEOUT);
  }

  /**
   * Set connection timeout of ots client
   * @param connectionTimeout
   */
  public void setOTSConnectionTimeout(int connectionTimeout) {
    setInt(TABLESTORE_CLIENT_CONNECTION_TIMEOUT, connectionTimeout);
  }

  /**
   * Get the timeout for an operation
   * @return the operation timeout
   */
  public int getOperationTimeout() {
    return getInt(TABLESTORE_CLIENT_OPERATION_TIMEOUT, DEFAULT_TABLESTORE_CLIENT_OPERATION_TIMEOUT);
  }

  /**
   * Set the timeout of operation
   * @param timeout
   */
  public void setOperationTimeout(int timeout) {
    setInt(TABLESTORE_CLIENT_OPERATION_TIMEOUT, timeout);
  }

  /**
   * Get the retry number for failed request
   * @return the retry number
   */
  public int getRetryCount() {
    return getInt(TABLESTORE_CLIENT_RETRIES, DEFAULT_TABLESTORE_CLIENT_RETRIES);
  }

  /**
   * Set the retry number for failed request
   * @param retryCount
   */
  public void setRetryCount(int retryCount) {
    setInt(TABLESTORE_CLIENT_RETRIES, retryCount);
  }

  public int getOTSMaxBatchRowCount() {
    return getInt(TABLESTORE_MAX_BATCH_ROW_COUNT, DEFAULT_TABLESTORE_MAX_BATCH_ROW_COUNT);
  }

  /**
   * Rely on the server configuration. Do not use it unauthorized.
   * @param rowCount
   */
  public void setOTSMaxBatchRowCount(int rowCount) {
    setInt(TABLESTORE_MAX_BATCH_ROW_COUNT, rowCount);
  }

  public int getOTSMaxBatchDataSize() {
    return getInt(TABLESTORE_MAX_BATCH_DATA_SIZE, DEFAULT_TABLESTORE_MAX_BATCH_DATA_SIZE);
  }

  /**
   * Rely on the server configuration. Do not use it unauthorized.
   * @param dataSize
   */
  public void setOTSMaxBatchDataSize(int dataSize) {
    setInt(TABLESTORE_MAX_BATCH_DATA_SIZE, dataSize);
  }

  public int getOTSMaxScanLimit() {
    return getInt(TABLESTORE_MAX_SCAN_LIMIT, DEFAULT_TABLESTORE_MAX_SCAN_LIMIT);
  }

  /**
   * Rely on the server configuration. Do not use it unauthorized.
   * @param limit
   */
  public void setOTSMaxScanLimit(int limit) {
    setInt(TABLESTORE_MAX_SCAN_LIMIT, limit);
  }

  public void setValue(String key, String value) {
    if (value == null) {
      this.properties.remove(key);
    } else {
      this.properties.setProperty(key, value);
    }
  }

  public String getValue(String key, String defaultValue) {
    return this.properties.getProperty(key, defaultValue);
  }

  public String getValue(String key) {
    return getValue(key, null);
  }

  public void setInt(String key, int value) {
    setValue(key, Integer.toString(value));
  }

  public int getInt(String key, int defaultValue) {
    String value = getValue(key);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return defaultValue;
  }

  public void setBoolean(String key, boolean value) {
    setValue(key, Boolean.toString(value));
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String value = getValue(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

}

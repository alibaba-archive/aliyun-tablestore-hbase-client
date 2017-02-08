/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import com.alicloud.tablestore.adaptor.struct.OTableDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This class defines all API of client accessing the OTS Service
 */
public interface OTSInterface extends Closeable {

  /**
   * Method for getting data from a row. If the row cannot be found an empty Result is returned.
   * This can be checked by the empty field of the OResult
   * @param tableName
   * @param get
   * @return the result of row
   */
  @Idempotent
  public com.alicloud.tablestore.adaptor.struct.OResult get(String tableName, com.alicloud.tablestore.adaptor.struct.OGet get) throws IOException;

  /**
   * Method for getting multiple rows. If a row cannot be found there will be a null value in the
   * result list for that OGet at the same position. So the Results are in the same order as the
   * OGets.
   * @param tableName
   * @param gets
   * @return the results of rows
   * @throws IOException
   */
  @Idempotent
  public List<com.alicloud.tablestore.adaptor.struct.OResult> getMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OGet> gets) throws IOException;

  /**
   * Method for scan the range rows as the specified OScan. You could set the start/stop row and
   * filter in the OScan. The most count of rows to return is specified by limit
   * @param tableName
   * @param scan
   * @param limit the most rows count to return
   * @return the results
   * @throws IOException
   */
  @Idempotent
  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit) throws IOException;

  /**
   * Method for scan the range rows as the specified OScan. You could set the start/stop row and
   * filter in the OScan. The most count of rows to return is specified by limit
   * @param tableName
   * @param scan
   * @param limit the most rows count to return
   * @param nextRow return the next row key if there are more rows between start row and end row.
   * @return the results
   * @throws IOException
   */
  @Idempotent
  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit, ByteArrayOutputStream nextRow) throws IOException;

  /**
   * Method for putting data of a row to the specified table.
   * @param tableName
   * @param put
   */
  @Idempotent
  public void put(String tableName, com.alicloud.tablestore.adaptor.struct.OPut put) throws IOException;

  /**
   * Method for putting data of multiple rows to the specified table.
   * @param tableName
   * @param puts
   * @throws IOException
   */
  @Idempotent
  public void putMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OPut> puts) throws IOException;

  /**
   * Method for deleting data of a row from the specified table. The deleted columns is specified in
   * the given ODelete.
   * @param tableName
   * @param delete
   * @throws IOException
   */
  @Idempotent
  public void delete(String tableName, com.alicloud.tablestore.adaptor.struct.ODelete delete) throws IOException;

  /**
   * Method for deleting data of multiple row from the specified table. This returns a list of
   * ODeletes that were not executed. So if everything succeeds you'll receive an empty list.
   * @param tableName
   * @param deletes
   * @throws IOException
   */
  @Idempotent
  public void deleteMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.ODelete> deletes) throws IOException;

  /**
   * Method for put or delete data of a row to the specified table.
   * @param tableName
   * @param update
   */
  @Idempotent
  public void update(String tableName, com.alicloud.tablestore.adaptor.struct.OUpdate update) throws IOException;

  /**
   * Method that does a batch call on ODeletes, OGets, OPuts. The ordering of execution of the
   * actions is not defined. Meaning if you do a Put and a Get in the same {@link #batch} call, you
   * will not necessarily be guaranteed that the Get returns what the Put had put.
   * @param tableName
   * @param actions list of OGet, OPut, ODelete objects
   * @param results Empty Object[], same size as actions. Provides access to partial results, in
   *          case an exception is thrown. A null in the result array means that the call for that
   *          action failed, even after retries
   * @throws IOException
   */
  @Idempotent
  public void batch(String tableName, final List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions, final Object[] results)
      throws IOException;

  /**
   * Same as {@link #batch(String, List, Object[])}, but returns an array of
   * results instead of using a results parameter reference.
   * @param tableName
   * @param actions
   * @return the results from the actions. They are OResult objects. The call
   *         will throw exception if one of the actions failed, even after
   *         retries.
   * @throws IOException
   */
  @Idempotent
  public Object[] batch(String tableName, final List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions) throws IOException;

  /**
   * List tables name of instance
   * @return table name list
   * @throws IOException
   */
  public List<String> listTable() throws IOException;

  /**
   * Create a TableStore table
   * @throws IOException
   */
  public void createTable(OTableDescriptor descriptor) throws IOException;

  /**
   * delete a TableStore table
   * @param tableName
   * @throws IOException
   */
  public void deleteTable(String tableName) throws IOException;

  /**
   * describe table
   * @param tableName
   * @return
   * @throws IOException
   */
  public OTableDescriptor describeTable(String tableName) throws IOException;

  /**
   * Update table parameter, such as MaxVersion, TimeToLive
   * @param descriptor
   * @throws IOException
   */
  public void updateTable(OTableDescriptor descriptor) throws IOException;
}

/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;
import com.alicloud.tablestore.adaptor.filter.OColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;

/**
 * Used to perform Get operations on a single row.
 * <p/>
 * To get everything for a row, instantiate a Get object with the row to get. To further define the
 * scope of what to get, perform additional methods as outlined below.
 * <p/>
 * To get specific columns, execute {@link #addColumn(byte[]) addColumn} for each column to
 * retrieve.
 * <p/>
 * To only retrieve columns within a specific range of version timestamps, execute
 * {@link #setTimeRange(long, long) setTimeRange}.
 * <p/>
 * To only retrieve columns with a specific timestamp, execute {@link #setTimeStamp(long)
 * setTimestamp}.
 * <p/>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p/>
 * To add a filter, execute {@link #setFilter(com.alicloud.tablestore.adaptor.filter.OFilter) setFilter}.
 */
public class OGet implements com.alicloud.tablestore.adaptor.struct.ORow, Comparable<com.alicloud.tablestore.adaptor.struct.ORow> {

  private byte[] row = null;
  private int maxVersions = 1;

  private com.alicloud.tablestore.adaptor.struct.OTimeRange tr = new com.alicloud.tablestore.adaptor.struct.OTimeRange();
  private NavigableSet<byte[]> columnsToGet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  private com.alicloud.tablestore.adaptor.filter.OFilter filter = null;

  /**
   * Create a Get operation for the specified row.
   * <p/>
   * If no further operations are done, this will get the latest version of all columns of the
   * specified row.
   * @param row row key
   */
  public OGet(byte[] row) {
    this.row = row;
  }

  /**
   * Builds a get object with the same specs as scan for the specified row
   * @param scan scan to model get after
   */
  public OGet(OScan scan, byte[] row) {
    this.row = row;
    this.filter = scan.getFilter();
    this.maxVersions = scan.getMaxVersions();
    this.tr = scan.getTimeRange();
    this.columnsToGet = scan.getColumnsToGet();
  }

  /**
   * Get the column with the specified qualifier.
   * @param qualifier column qualifier
   * @return the Get objec
   */
  public OGet addColumn(byte[] qualifier) {
    this.columnsToGet.add(qualifier);
    return this;
  }

  /**
   * Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @return this for invocation chaining
   * @throws IOException if invalid time range
   */
  public OGet setTimeRange(long minStamp, long maxStamp) throws IOException {
    tr = new com.alicloud.tablestore.adaptor.struct.OTimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public OGet setTimeStamp(long timestamp) throws IOException {
    tr = new com.alicloud.tablestore.adaptor.struct.OTimeRange(timestamp, timestamp + 1);
    return this;
  }

  /**
   * Get all available versions.
   * @return this for invocation chaining
   */
  public OGet setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @return this for invocation chaining
   * @throws IOException if invalid number of versions
   */
  public OGet setMaxVersions(int maxVersions) throws IOException {
    if (maxVersions <= 0) {
      throw new IOException("maxVersions must be positive");
    }
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Apply the specified server-side filter when performing the Get.
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public OGet setFilter(com.alicloud.tablestore.adaptor.filter.OFilter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * @return Filter
   */
  public com.alicloud.tablestore.adaptor.filter.OFilter getFilter() {
    return this.filter;
  }

  /**
   * Method for retrieving the get's row
   * @return row
   */
  public byte[] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the get's maximum number of version
   * @return the maximum number of version to fetch for this get
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * Method for retrieving the get's TimeRange
   * @return timeRange
   */
  public com.alicloud.tablestore.adaptor.struct.OTimeRange getTimeRange() {
    return this.tr;
  }

  public int compareTo(com.alicloud.tablestore.adaptor.struct.ORow other) {
    return Bytes.compareTo(this.getRow(), other.getRow());
  }

  public NavigableSet<byte[]> getColumnsToGet() {
    return this.columnsToGet;
  }

  /**
   * For core use. DO NOT USE.
   */
  public SingleRowQueryCriteria toOTSParameter(String tableName) {
    PrimaryKey primaryKey = OTSUtil.toPrimaryKey(getRow(), OTSConstants.PRIMARY_KEY_NAME);
    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
    criteria.setMaxVersions(getMaxVersions());
    criteria.setTimeRange(OTSUtil.toTimeRange(getTimeRange()));
    for (byte[] col : columnsToGet) {
      criteria.addColumnsToGet(Bytes.toString(col));
    }
    if (filter != null) {
      if (filter instanceof OColumnPaginationFilter) {
        OColumnPaginationFilter oFilter = (OColumnPaginationFilter)filter;
        com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter columnPaginationFilter =
                new com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter(oFilter.getLimit());
        if (oFilter.getColumnOffset() == null) {
          columnPaginationFilter.setOffset(oFilter.getOffset());
        } else {
          criteria.setStartColumn(Bytes.toString(oFilter.getColumnOffset()));
        }
        criteria.setFilter(columnPaginationFilter);
      } else {
        criteria.setFilter(OTSUtil.toFilter(getFilter()));
      }
    }
    return criteria;
  }

}

/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.tablestore.adaptor.client.OClientScanner;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.filter.OColumnPaginationFilter;

/**
 * Used to perform Scan operations.
 * <p/>
 * All operations are identical to {@link OScan} with the exception of instantiation. Rather than
 * specifying a single row, an optional startRow and stopRow may be defined. If rows are not
 * specified, the Scanner will iterate over all rows.
 * <p/>
 * To scan everything for each row, instantiate a Scan object.
 * <p/>
 * To modify scanner caching for just this scan, use {@link #setCaching(int) setCaching}. If caching
 * is NOT set, we will use the caching value . See
 * {@link OClientScanner#DEFAULT_CLIENT_SCANNER_CACHING}.
 * <p/>
 * To further define the scope of what to get when scanning, perform additional methods as outlined
 * below.
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
public class OScan {
  private byte[] startRow = com.alicloud.tablestore.adaptor.client.OTSConstants.EMPTY_START_ROW;
  private byte[] stopRow = com.alicloud.tablestore.adaptor.client.OTSConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private com.alicloud.tablestore.adaptor.filter.OFilter filter = null;

  /*
   * -1 means no caching
   */
  private int caching = -1;
  private OTimeRange tr = new OTimeRange();
  private NavigableSet<byte[]> columnsToGet = new TreeSet<byte[]>(com.alicloud.tablestore.adaptor.client.util.Bytes.BYTES_COMPARATOR);
  private boolean reversed = false;

  /**
   * Create a Scan operation across all rows.
   */
  public OScan() {
  }

  /**
   * Create a Scan operation starting at the specified row.
   * <p/>
   * If the specified row does not exist, the Scanner will start from the next closest row after the
   * specified row.
   * @param startRow row to start scanner at or after
   */
  public OScan(byte[] startRow) {
    this.startRow = startRow;
  }

  public OScan(byte[] startRow, com.alicloud.tablestore.adaptor.filter.OFilter filter) {
    this(startRow);
    this.filter = filter;
  }

  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public OScan(byte[] startRow, byte[] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  /**
   * Creates a new instance of this class while copying all values.
   * @param scan The scan instance to copy from.
   * @throws IOException When copying the values fails.
   */
  public OScan(OScan scan) throws IOException {
    startRow = scan.getStartRow();
    stopRow = scan.getStopRow();
    maxVersions = scan.getMaxVersions();
    caching = scan.getCaching();
    OTimeRange ctr = scan.getTimeRange();
    tr = new OTimeRange(ctr.getMin(), ctr.getMax());
    NavigableSet<byte[]> columns = scan.getColumnsToGet();
    this.columnsToGet.addAll(columns);
  }

  /**
   * Get the column from the specified qualifier.
   * @param qualifier column qualifier
   * @return this
   */
  public OScan addColumn(byte[] qualifier) {
    this.columnsToGet.add(qualifier);

    return this;
  }

  /**
   * Get versions of columns only within the specified timestamp range, [minStamp, maxStamp). Note,
   * default maximum versions to return is 1. If your time range spans more than one version and you
   * want all versions returned, up the number of versions beyond the defaut.
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @return this
   * @throws IOException if invalid time range
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   */
  public OScan setTimeRange(long minStamp, long maxStamp) throws IOException {
    tr = new OTimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp. Note, default maximum versions to return
   * is 1. If your time range spans more than one version and you want all versions returned, up the
   * number of versions beyond the defaut.
   * @param timestamp version timestamp
   * @return this
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   */
  public OScan setTimeStamp(long timestamp) {
    try {
      tr = new OTimeRange(timestamp, timestamp + 1);
    } catch (IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Set the start row of the scan.
   * @param startRow row to start scan on (inclusive) Note: In order to make startRow exclusive add a
   *          trailing 0 byte
   * @return this
   */
  public OScan setStartRow(byte[] startRow) {
    this.startRow = startRow;
    return this;
  }

  /**
   * Set the stop row.
   * @param stopRow row to end at (exclusive) Note: In order to make stopRow inclusive add a trailing
   *          0 byte
   * @return this
   */
  public OScan setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
    return this;
  }

  /**
   * Get all available versions.
   * @return this
   */
  public OScan setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @return this
   */
  public OScan setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners.
   * Higher caching values will enable
   * faster scanners but will use more memory.
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }

  /**
   * Apply the specified server-side filter when performing the Scan.
   * @param filter filter to run on the server
   * @return this
   */
  public OScan setFilter(com.alicloud.tablestore.adaptor.filter.OFilter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * set columns to get.
   * @param columnsToGet
   * @return
   */
  public OScan setColumnsToGet(NavigableSet<byte[]> columnsToGet) {
    this.columnsToGet = columnsToGet;
    return this;
  }

  /**
   * Set whether this scan is a reversed one.
   * <p/>
   * This is false by default which means forward(normal) scan.
   * @param reversed if true, scan will be backward order
   */
  public void setReversed(boolean reversed) {
    this.reversed = reversed;
  }

  /**
   * @return the startrow
   */
  public byte[] getStartRow() {
    return this.startRow;
  }

  /**
   * @return the stoprow
   */
  public byte[] getStopRow() {
    return this.stopRow;
  }

  /**
   * @return the max number of versions to fetch
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * @return caching the number of rows fetched when calling next on a scanner
   */
  public int getCaching() {
    return this.caching;
  }

  /**
   * @return TimeRange
   */
  public OTimeRange getTimeRange() {
    return this.tr;
  }

  public NavigableSet<byte[]> getColumnsToGet() {
    return this.columnsToGet;
  }

  /**
   * @return RowFilter
   */
  public com.alicloud.tablestore.adaptor.filter.OFilter getFilter() {
    return filter;
  }

  /**
   * @return true is a filter has been specified, false if not
   */
  public boolean hasFilter() {
    return filter != null;
  }

  /**
   * Get whether this scan is a reverse one.
   * @return true if backward scan, false if forward(default) scan
   */
  public boolean getReversed() {
    return reversed;
  }

  /**
   * For core use. DO NOT USE.
   */
  public RangeRowQueryCriteria toOTSParameter(String tableName) {
    RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
    criteria.setMaxVersions(getMaxVersions());
    criteria.setTimeRange(com.alicloud.tablestore.adaptor.client.util.OTSUtil.toTimeRange(getTimeRange()));
    for (byte[] col : columnsToGet) {
      criteria.addColumnsToGet(com.alicloud.tablestore.adaptor.client.util.Bytes.toString(col));
    }
    if (getStartRow().length == 0) {
      List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
      if (!getReversed()) {
        pks.add(new PrimaryKeyColumn(com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME, PrimaryKeyValue.INF_MIN));
      } else {
        pks.add(new PrimaryKeyColumn(com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME, PrimaryKeyValue.INF_MAX));
      }
      criteria.setInclusiveStartPrimaryKey(new PrimaryKey(pks));
    } else {
      criteria.setInclusiveStartPrimaryKey(com.alicloud.tablestore.adaptor.client.util.OTSUtil.toPrimaryKey(getStartRow(),
              com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME));
    }
    if (getStopRow().length == 0) {
      List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
      if (!getReversed()) {
        pks.add(new PrimaryKeyColumn(com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME, PrimaryKeyValue.INF_MAX));
      } else {
        pks.add(new PrimaryKeyColumn(com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME, PrimaryKeyValue.INF_MIN));
      }
      criteria.setExclusiveEndPrimaryKey(new PrimaryKey(pks));
    } else {
      criteria
              .setExclusiveEndPrimaryKey(com.alicloud.tablestore.adaptor.client.util.OTSUtil.toPrimaryKey(getStopRow(), com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME));
    }
    if (getCaching() > 0) {
      criteria.setLimit(getCaching());
    }
    if (hasFilter()) {
      if (filter instanceof OColumnPaginationFilter) {
        com.alicloud.tablestore.adaptor.filter.OColumnPaginationFilter oFilter = (OColumnPaginationFilter) filter;
        com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter columnPaginationFilter =
                new com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter(oFilter.getLimit());
        if (oFilter.getColumnOffset() == null) {
          columnPaginationFilter.setOffset(oFilter.getOffset());
        } else {
          criteria.setStartColumn(Bytes.toString(oFilter.getColumnOffset()));
        }
        criteria.setFilter(columnPaginationFilter);
      } else{
          criteria.setFilter(com.alicloud.tablestore.adaptor.client.util.OTSUtil.toFilter(getFilter()));
      }
    }

    if (getReversed()) {
      criteria.setDirection(Direction.BACKWARD);
    }
    return criteria;
  }
}

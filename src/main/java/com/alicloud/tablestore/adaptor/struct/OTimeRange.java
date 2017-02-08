/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.io.IOException;

import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;

/**
 * Represents an interval of version timestamps.
 * <p/>
 * Evaluated according to minStamp <= timestamp < maxStamp or [minStamp,maxStamp) in interval
 * notation.
 * <p/>
 * Only used internally; should not be accessed directly by clients.
 */
public class OTimeRange {
  private long minStamp = 0L;
  private long maxStamp = OTSConstants.LATEST_TIMESTAMP;
  private boolean allTime = false;

  /**
   * Default constructor. Represents interval [0, Long.MAX_VALUE) (allTime)
   */
  public OTimeRange() {
    allTime = true;
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public OTimeRange(long minStamp) {
    this.minStamp = minStamp;
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public OTimeRange(byte[] minStamp) {
    this.minStamp = Bytes.toLong(minStamp);
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IOException
   */
  public OTimeRange(long minStamp, long maxStamp) throws IOException {
    if (maxStamp < minStamp) {
      throw new IOException("maxStamp is smaller than minStamp");
    }
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IOException
   */
  public OTimeRange(byte[] minStamp, byte[] maxStamp) throws IOException {
    this(Bytes.toLong(minStamp), Bytes.toLong(maxStamp));
  }

  /**
   * @return the smallest timestamp that should be considered
   */
  public long getMin() {
    return minStamp;
  }

  /**
   * @return the biggest timestamp that should be considered
   */
  public long getMax() {
    return maxStamp;
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p/>
   * Returns true if within interval [minStamp, maxStamp), false if not.
   * @param bytes timestamp to check
   * @param offset offset into the bytes
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(byte[] bytes, int offset) {
    if (allTime) return true;
    return withinTimeRange(Bytes.toLong(bytes, offset));
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p/>
   * Returns true if within interval [minStamp, maxStamp), false if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(long timestamp) {
    if (allTime) return true;
    // check if >= minStamp
    return (minStamp <= timestamp && timestamp < maxStamp);
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p/>
   * Returns true if within interval [minStamp, maxStamp), false if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinOrAfterTimeRange(long timestamp) {
    if (allTime) return true;
    // check if >= minStamp
    return (timestamp >= minStamp);
  }

  /**
   * Compare the timestamp to timerange
   * @param timestamp
   * @return -1 if timestamp is less than timerange, 0 if timestamp is within timerange, 1 if
   *         timestamp is greater than timerange
   */
  public int compare(long timestamp) {
    if (timestamp < minStamp) {
      return -1;
    } else if (timestamp >= maxStamp) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("maxStamp=");
    sb.append(this.maxStamp);
    sb.append(", minStamp=");
    sb.append(this.minStamp);
    return sb.toString();
  }
}

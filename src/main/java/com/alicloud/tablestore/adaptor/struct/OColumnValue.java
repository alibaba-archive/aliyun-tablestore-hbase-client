/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.util.Comparator;

import com.alicloud.tablestore.adaptor.client.util.Bytes;

/**
 * Represents a single column and its value.
 */
public class OColumnValue {
  public static Comparator<OColumnValue> KEY_COMPARATOR = new Comparator<OColumnValue>() {
    @Override
    public int compare(OColumnValue k1, OColumnValue k2) {
      int ret = Bytes.compareTo(k1.getQualifier(), k2.getQualifier());
      if (ret != 0) {
        return ret;
      }

      long t1 = k1.getTimestamp();
      long t2 = k2.getTimestamp();
      return t1 == t2 ? 0 : (t1 < t2 ? 1 : -1);
    }
  };

  public static enum Type {
    PUT, DELETE, DELETE_ALL
  }

  private Type type;
  private byte[] row;
  private byte[] qualifier;
  private byte[] value;
  private long timestamp;

  public OColumnValue(byte[] row, byte[] qualifier, long ts, Type type, byte[] value) {
    this.row = row;
    this.qualifier = qualifier;
    this.timestamp = ts;
    this.type = type;
    this.value = value;
  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public byte[] getValue() {
    return value;
  }

  public Type getType() {
    return type;
  }

  /**
   * Needed doing 'contains' on List.  Only compares the key portion, not the value.
   * @param other
   * @return
   */
  public boolean equals(Object other) {
    if (!(other instanceof OColumnValue)) {
      return false;
    }
    if (!Bytes.equals(getRow(), ((OColumnValue) other).getRow())) {
      return false;
    }
    if (!Bytes.equals(getQualifier(), ((OColumnValue) other).getQualifier())) {
      return false;
    }
    if (getTimestamp() != ((OColumnValue) other).getTimestamp()) {
      return false;
    }
    if (getType() != ((OColumnValue) other).getType()) {
      return false;
    }
    return true;
  }

}

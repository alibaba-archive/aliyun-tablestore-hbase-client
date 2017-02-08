/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;

public class OPut extends OMutation implements Comparable<ORow> {

  /**
   * Create a Put operation for the specified row.
   * @param row row key
   */
  public OPut(byte[] row) {
    this(row, OTSConstants.LATEST_TIMESTAMP);
  }

  /**
   * Create a Put operation for the specified row, using a given timestam.
   * @param row row key
   * @param ts timestamp
   */
  public OPut(byte[] row, long ts) {
    if (row == null) {
      throw new IllegalArgumentException("Row key is invalid");
    }
    this.row = Arrays.copyOf(row, row.length);
    this.ts = ts;
  }

  /**
   * Copy constructor. Creates a Put operation cloned from the specified Put.
   * @param putToCopy put to copy
   */
  public OPut(OPut putToCopy) {
    this(putToCopy.getRow(), putToCopy.ts);
    this.keyValues.addAll(putToCopy.keyValues);
  }

  /**
   * Add the specified column and value to this Put operation.
   * @param qualifier column qualifier
   * @param value column value
   * @return this
   */
  public OPut add(byte[] qualifier, byte[] value) {
    return add(qualifier, this.ts, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as its version to this Put
   * operation.
   * @param qualifier column qualifier
   * @param ts version timestamp
   * @param value column value
   * @return this
   */
  public OPut add(byte[] qualifier, long ts, byte[] value) {
    com.alicloud.tablestore.adaptor.struct.OColumnValue kv = createPutKeyValue(qualifier, ts, value);
    keyValues.add(kv);
    return this;
  }

  /**
   * Add the specified KeyValue to this Put operation. Operation assumes that the passed KeyValue is
   * immutable and its backing array will not be modified for the duration of this Put.
   * @param kv individual KeyValue
   * @return this
   * @throws java.io.IOException e
   */
  public OPut add(com.alicloud.tablestore.adaptor.struct.OColumnValue kv) throws IOException {
    // Checking that the row of the kv is the same as the put
    int res = Bytes.compareTo(this.row, 0, row.length, kv.getRow(), 0, kv.getRow().length);
    if (res != 0) {
      throw new IOException("The row in the recently added KeyValue "
          + Bytes.toStringBinary(kv.getRow(), 0, kv.getRow().length)
          + " doesn't match the original one " + Bytes.toStringBinary(this.row));
    }

    keyValues.add(kv);
    return this;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  private com.alicloud.tablestore.adaptor.struct.OColumnValue createPutKeyValue(byte[] qualifier, long ts, byte[] value) {
    return new com.alicloud.tablestore.adaptor.struct.OColumnValue(this.row, qualifier, ts, com.alicloud.tablestore.adaptor.struct.OColumnValue.Type.PUT, value);
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family & qualifier. Both given arguments must match the KeyValue object to return true.
   * @param qualifier column qualifier
   * @return returns true if the given family and qualifier already has an existing KeyValue object
   *         in the family map.
   */
  public boolean has(byte[] qualifier) {
    return has(qualifier, this.ts, new byte[0], true, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family, qualifier and timestamp. All 3 given arguments must match the KeyValue object to
   * return true.
   * @param qualifier column qualifier
   * @param ts timestamp
   * @return returns true if the given family, qualifier and timestamp already has an existing
   *         KeyValue object in the family map.
   */
  public boolean has(byte[] qualifier, long ts) {
    return has(qualifier, ts, new byte[0], false, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family, qualifier and timestamp. All 3 given arguments must match the KeyValue object to
   * return true.
   * @param qualifier column qualifier
   * @param value value to check
   * @return returns true if the given family, qualifier and value already has an existing KeyValue
   *         object in the family map.
   */
  public boolean has(byte[] qualifier, byte[] value) {
    return has(qualifier, this.ts, value, true, false);
  }

  /**
   * A convenience method to determine if this object's familyMap contains the given value assigned
   * to the given family, qualifier and timestamp. All 4 given arguments must match the KeyValue
   * object to return true.
   * @param qualifier column qualifier
   * @param ts timestamp
   * @param value value to check
   * @return returns true if the given family, qualifier timestamp and value already has an existing
   *         KeyValue object in the family map.
   */
  public boolean has(byte[] qualifier, long ts, byte[] value) {
    return has(qualifier, ts, value, false, false);
  }

  /*
   * Private method to determine if this object's familyMap contains the given value assigned to the
   * given family, qualifier and timestamp respecting the 2 boolean arguments
   * @param family
   * @param qualifier
   * @param ts
   * @param value
   * @param ignoreTS
   * @param ignoreValue
   * @return returns true if the given family, qualifier timestamp and value already has an existing
   * KeyValue object in the family map.
   */
  private boolean
      has(byte[] qualifier, long ts, byte[] value, boolean ignoreTS, boolean ignoreValue) {
    if (keyValues.size() == 0) {
      return false;
    }
    // Boolean analysis of ignoreTS/ignoreValue.
    // T T => 2
    // T F => 3 (first is always true)
    // F T => 2
    // F F => 1
    if (!ignoreTS && !ignoreValue) {
      for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
        if (Arrays.equals(kv.getQualifier(), qualifier) && Arrays.equals(kv.getValue(), value)
            && kv.getTimestamp() == ts) {
          return true;
        }
      }
    } else if (ignoreValue && !ignoreTS) {
      for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
        if (Arrays.equals(kv.getQualifier(), qualifier) && kv.getTimestamp() == ts) {
          return true;
        }
      }
    } else if (!ignoreValue && ignoreTS) {
      for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
        if (Arrays.equals(kv.getQualifier(), qualifier) && Arrays.equals(kv.getValue(), value)) {
          return true;
        }
      }
    } else {
      for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
        if (Arrays.equals(kv.getQualifier(), qualifier)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns a list of all KeyValue objects with matching column family and qualifier.
   * @param qualifier column qualifier
   * @return a list of KeyValue objects with the matching family and qualifier, returns an empty
   *         list if one doesnt exist for the given family.
   */
  public List<com.alicloud.tablestore.adaptor.struct.OColumnValue> get(byte[] qualifier) {
    List<com.alicloud.tablestore.adaptor.struct.OColumnValue> filteredList = new ArrayList<com.alicloud.tablestore.adaptor.struct.OColumnValue>();
    for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
      if (Arrays.equals(kv.getQualifier(), qualifier)) {
        filteredList.add(kv);
      }
    }
    return filteredList;
  }

  /**
   * Creates an empty list if one doesnt exist for the given column family or else it returns the
   * associated list of KeyValue objects.
   * @return a list of KeyValue objects, returns an empty list if one doesnt exist.
   */
  public List<com.alicloud.tablestore.adaptor.struct.OColumnValue> getKeyValueList() {
    return this.keyValues;
  }

  /**
   * @return
   */
  public long getWritableSize() {
    long size = OTSConstants.PRIMARY_KEY_NAME.length() + this.row.length;
    for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
      size += kv.getQualifier().length;
      size += kv.getValue().length;
      size += 8;
    }
    return size;
  }

  /**
   * For core use. DO NO USE.
   */
  public RowUpdateChange toOTSParameter(String tableName) {
    PrimaryKey primaryKey = OTSUtil.toPrimaryKey(getRow(), OTSConstants.PRIMARY_KEY_NAME);
    RowUpdateChange ruc = new RowUpdateChange(tableName, primaryKey);
    for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
      Column column = null;
      if (kv.getTimestamp() == OTSConstants.LATEST_TIMESTAMP) {
        column = new Column(Bytes.toString(kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()));
      } else {
        column = new Column(Bytes.toString(kv.getQualifier()), ColumnValue.fromBinary(kv.getValue()),
                kv.getTimestamp());
      }
      ruc.put(column);
    }
    ruc.setCondition(getCondition());
    return ruc;
  }
}

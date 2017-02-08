/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import java.util.*;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;

/**
 * Single row result of a Get or Scan query.
 * <p/>
 * <p/>
 * This class is NOT THREAD SAFE.
 * <p/>
 * <p/>
 * Convenience methods are available that return various {@link Map} structures and values directly.
 * <p/>
 * <p/>
 * To get a complete mapping of all cells in the Result, which can include multiple families and
 * multiple versions, use {@link #getMap()}.
 * <p/>
 * <p/>
 * To get a mapping of each family to its columns (qualifiers and values), including only the latest
 * version of each, use {@link #getNoVersionMap()}.
 * <p/>
 * To get a mapping of qualifiers to latest values for an individual family use
 * {@link (byte[])}.
 * <p/>
 * <p/>
 * To get the latest value for a specific family and qualifier use}.
 * <p/>
 * A Result is backed by an array of {@link OColumnValue} objects, each representing a column value
 * cell defined by the family, qualifier, timestamp, and value.
 * <p/>
 * <p/>
 * The underlying {@link OColumnValue} objects can be accessed through the method {@link #list()}.
 * Each HColumnValue can then be accessed through {@link OColumnValue#getQualifier()},
 * {@link OColumnValue#getTimestamp()}, and {@link OColumnValue#getValue()}.
 */
public class OResult {

  private OColumnValue[] kvs = null;
  private NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = null;
  // We're not using java serialization. Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  private transient byte[] row = null;
  private Response response = null;

  /**
   * Instantiate a Result with the specified array of KeyValues.
   * @param kvs array of KeyValues
   */
  public OResult(OColumnValue[] kvs) {
    if (kvs == null) {
      this.kvs = new OColumnValue[0];
    } else {
      this.kvs = kvs;
    }
  }

  /**
   * Instantiate a Result with the specified List of KeyValues.
   * @param kvs List of KeyValues
   */
  public OResult(List<OColumnValue> kvs) {
    this(kvs.toArray(new OColumnValue[kvs.size()]));
  }

  /**
   * Method for retrieving the row key that corresponds to the row from which this Result was
   * created.
   * @return row
   */
  public byte[] getRow() {
    if (this.row == null) {
      this.row = (this.kvs == null || this.kvs.length == 0) ? null : this.kvs[0].getRow();
    }
    return this.row;
  }

  /**
   * Return the array of KeyValues backing this Result instance.
   * <p/>
   * The array is sorted with qualifier from smallest -> largest and timestamp from newest ->
   * oldest.
   * <p/>
   * The array only contains what your Get or Scan specifies and no more. For example if you request
   * column "A" 1 version you will have at most 1 KeyValue in the array. If you request column "A"
   * with 2 version you will have at most 2 KeyValues, with the first one being the newer timestamp
   * and the second being the older timestamp. If columns don't exist, they won't be present in the
   * result. Therefore if you ask for 1 version all columns, it is safe to iterate over this array
   * and expect to see 1 KeyValue for each column and no more.
   * <p/>
   * This API is faster than using getMap()
   * @return array of KeyValues
   */
  public OColumnValue[] raw() {
    return kvs;
  }

  /**
   * Create a sorted list of the KeyValue's in this result.
   * <p/>
   * Since HBase 0.20.5 this is equivalent to raw().
   * @return The sorted list of KeyValue's.
   */
  public List<OColumnValue> list() {
    return Arrays.asList(raw());
  }

  /**
   * Return the KeyValues for the specific column. The KeyValues are sorted with qualifier from
   * smallest -> largest and timestamp from newest -> oldest. That implies the first entry in the
   * list is the most recent column. If the query (Scan or Get) only requested 1 version the list
   * will contain at most 1 entry. If the column did not exist in the result set (either the column
   * does not exist or the column was not selected in the query) the list will be empty.
   * <p/>
   * Also see getColumnLatest which returns just a KeyValue
   * @param qualifier
   * @return a list of KeyValues for this column or empty list if the column did not exist in the
   *         result set
   */
  public List<OColumnValue> getColumn(byte[] qualifier) {
    List<OColumnValue> result = new ArrayList<OColumnValue>();

    if (kvs == null || kvs.length == 0) {
      return result;
    }

    int pos = binarySearch(kvs, qualifier);
    if (pos == -1) {
      return result; // cant find it
    }

    for (int i = pos; i < kvs.length; i++) {
      OColumnValue kv = kvs[i];
      if (Bytes.equals(kv.getQualifier(), qualifier)) {
        result.add(kv);
      } else {
        break;
      }
    }

    return result;
  }

  protected int binarySearch(final OColumnValue[] kvs, final byte[] qualifier) {
    OColumnValue searchTerm =
        new OColumnValue(kvs[0].getRow(), qualifier, OTSConstants.LATEST_TIMESTAMP,
            OColumnValue.Type.PUT, null);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, OColumnValue.KEY_COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos + 1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * The KeyValue for the most recent for a given column. If the column does not exist in the result
   * set - if it wasn't selected in the query (Get/Scan) or just does not exist in the row the
   * return value is null.
   * @param qualifier
   * @return KeyValue for the column or null
   */
  public OColumnValue getColumnLatest(byte[] qualifier) {
    OColumnValue[] kvs = raw(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, qualifier);
    if (pos == -1) {
      return null;
    }
    OColumnValue kv = kvs[pos];
    if (Bytes.equals(kv.getQualifier(), qualifier)) {
      return kv;
    }
    return null;
  }

  /**
   * Get the latest version of the specified column.
   * @param qualifier column qualifier
   * @return value of latest version of column, null if none found
   */
  public byte[] getValue(byte[] qualifier) {
    OColumnValue kv = getColumnLatest(qualifier);
    if (kv == null) {
      return null;
    }
    return kv.getValue();
  }

  /**
   * Checks for existence of the specified column.
   * @param qualifier column qualifier
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte[] qualifier) {
    OColumnValue kv = getColumnLatest(qualifier);
    return kv != null;
  }

  /**
   * Map of all versions of its qualifiers and values.
   * <p/>
   * Returns a two level Map of the form: <code>Map&lt;qualifier,Map&lt;timestamp,value>></code>
   * <p/>
   * Note: All other map returning methods make use of this map internally.
   * @return map from families to qualifiers to versions
   */
  public NavigableMap<byte[], NavigableMap<Long, byte[]>> getMap() {
    if (this.columnMap != null) {
      return this.columnMap;
    }
    this.columnMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR);
    for (OColumnValue kv : this.kvs) {
      byte[] qualifier = kv.getQualifier();
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if (versionMap == null) {
        versionMap = new TreeMap<Long, byte[]>(new Comparator<Long>() {
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      versionMap.put(kv.getTimestamp(), kv.getValue());
    }
    return this.columnMap;
  }

  /**
   * Map of most recent qualifiers and values.
   * <p/>
   * Returns a Map of the form: <code>Map&lt;qualifier,value></code>
   * <p/>
   * The most recent version of each qualifier will be used.
   * @return map from families to qualifiers and value
   */
  public NavigableMap<byte[], byte[]> getNoVersionMap() {
    if (this.columnMap == null) {
      getMap();
    }
    NavigableMap<byte[], byte[]> returnMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : columnMap.entrySet()) {
      returnMap.put(qualifierEntry.getKey(),
        qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey()));
    }
    return returnMap;
  }

  /**
   * Returns the value of the first column in the Result.
   * @return value of the first column
   */
  public byte[] value() {
    if (isEmpty()) {
      return null;
    }
    return kvs[0].getValue();
  }

  /**
   * Check if the underlying KeyValue [] is empty or not
   * @return true if empty
   */
  public boolean isEmpty() {
    return this.kvs == null || this.kvs.length == 0;
  }

  /**
   * @return the size of the underlying KeyValue []
   */
  public int size() {
    return this.kvs == null ? 0 : this.kvs.length;
  }

  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keyvalues=");
    if (isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for (OColumnValue kv : this.kvs) {
      if (moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Copy another Result into this one. Needed for the old Mapred framework
   * @param other
   */
  public void copyFrom(OResult other) {
    this.row = other.row;
    this.columnMap = other.columnMap;
    this.kvs = other.kvs;
  }

  /**
   * Does a deep comparison of two Results, down to the byte arrays.
   * @param res1 first result to compare
   * @param res2 second result to compare
   * @throws Exception Every difference is throwing an exception
   */
  public static void compareResults(OResult res1, OResult res2) throws Exception {
    if (res2 == null) {
      throw new Exception("There wasn't enough rows, we stopped at "
          + Bytes.toStringBinary(res1.getRow()));
    }
    if (res1.size() != res2.size()) {
      throw new Exception("This row doesn't have the same number of KVs: " + res1.toString()
          + " compared to " + res2.toString());
    }
    OColumnValue[] ourKVs = res1.raw();
    OColumnValue[] replicatedKVs = res2.raw();
    for (int i = 0; i < res1.size(); i++) {
      if (!ourKVs[i].equals(replicatedKVs[i])
          || !Bytes.equals(ourKVs[i].getValue(), replicatedKVs[i].getValue())) {
        throw new Exception("This result was different: " + res1.toString() + " compared to "
            + res2.toString());
      }
    }
  }

  /**
   * For core use. DO NOT USE.
   * @param response contain request_id and trace id
   */
  public void setOtsResult(Response response) {
    this.response = response;
  }

  /**
   * For core use. DO NOT USE.
   * @return
   */
  public Response getOtsResult() {
    return response;
  }
}

/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;

public class ODelete extends OMutation implements Comparable<ORow> {

  /**
   * Create a Delete operation for the specified row.
   *
   * If no further operations are done, this will delete everything associated with the specified
   * row (all versions of all columns).
   * @param row row key
   */
  public ODelete(byte[] row) {
    this.row = row;
  }

  /**
   * @param d Delete to clone.
   */
  public ODelete(final ODelete d) {
    this.row = d.getRow();
    this.keyValues.addAll(d.getKeyValues());
  }

  /**
   * Delete all versions of the specified column.
   * @param qualifier column qualifier
   * @return this for invocation chaining
   */
  public ODelete deleteColumns(byte[] qualifier) {
    this.keyValues.add(new OColumnValue(this.row, qualifier, OTSConstants.LATEST_TIMESTAMP,
        OColumnValue.Type.DELETE_ALL, null));
    return this;
  }

  /**
   * Delete the specified version of the specified column.
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public ODelete deleteColumn(byte[] qualifier, long timestamp) {
    keyValues.add(new OColumnValue(this.row, qualifier, timestamp, OColumnValue.Type.DELETE, null));
    return this;
  }

  /**
   * For core use. DO NOT USE.
   */
  public RowChange toOTSParameter(String tableName) {
    PrimaryKey primaryKey = OTSUtil.toPrimaryKey(getRow(), OTSConstants.PRIMARY_KEY_NAME);
    if (keyValues.isEmpty()) {
      // delete row
      RowDeleteChange rdc = new RowDeleteChange(tableName, primaryKey);
      rdc.setCondition(getCondition());
      return rdc;
    } else {
      RowUpdateChange ruc = new RowUpdateChange(tableName, primaryKey);
      for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
        if (kv.getType() == com.alicloud.tablestore.adaptor.struct.OColumnValue.Type.DELETE) {
          ruc.deleteColumn(Bytes.toString(kv.getQualifier()), kv.getTimestamp());
        } else if (kv.getType() == com.alicloud.tablestore.adaptor.struct.OColumnValue.Type.DELETE_ALL) {
          ruc.deleteColumns(Bytes.toString(kv.getQualifier()));
        }
      }
      ruc.setCondition(getCondition());
      return ruc;
    }
  }

  /**
   * @return writeable size
   */
  public long getWritableSize() {
    long size = OTSConstants.PRIMARY_KEY_NAME.length() + this.row.length;
    for (com.alicloud.tablestore.adaptor.struct.OColumnValue kv : keyValues) {
      size += kv.getQualifier().length;
      size += 8;
    }
    return size;
  }

}
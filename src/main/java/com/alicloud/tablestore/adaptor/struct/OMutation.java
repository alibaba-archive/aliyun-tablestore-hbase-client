package com.alicloud.tablestore.adaptor.struct;

import com.alicloud.openservices.tablestore.model.Condition;

import java.util.ArrayList;
import java.util.List;

public abstract class OMutation implements ORow {
  protected byte[] row = null;
  protected long ts = com.alicloud.tablestore.adaptor.client.OTSConstants.LATEST_TIMESTAMP;
  protected List<OColumnValue> keyValues = new ArrayList<OColumnValue>();
  protected Condition condition = new Condition();

  public void setKeyValues(List<OColumnValue> keyValues) {
    com.alicloud.tablestore.adaptor.client.util.Preconditions.checkNotNull(keyValues);
    this.keyValues = keyValues;
  }

  public List<OColumnValue> getKeyValues() {
    return this.keyValues;
  }

  public void setCondition(Condition conditon) {
    this.condition = conditon;
  }

  public Condition getCondition() {
    return this.condition;
  }

  public boolean isEmpty() {
    return keyValues.isEmpty();
  }

  /**
   * Method for retrieving the delete's row
   * @return row
   */
  @Override
  public byte[] getRow() {
    return this.row;
  }

  public int compareTo(final ORow d) {
    return com.alicloud.tablestore.adaptor.client.util.Bytes.compareTo(this.getRow(), d.getRow());
  }

  /**
   * @return the total number of KeyValues
   */
  public int size() {
    return keyValues.size();
  }

  public List<OColumnValue> getColumn(byte[] qualifier) {
    List<OColumnValue> result = new ArrayList<OColumnValue>();
    for (OColumnValue kv : keyValues) {
      if (com.alicloud.tablestore.adaptor.client.util.Bytes.equals(kv.getQualifier(), qualifier)) {
        result.add(kv);
      }
    }
    return result;
  }
}

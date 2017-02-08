package com.alicloud.tablestore.adaptor.filter;

public class OSingleColumnValueFilter implements OFilter {

  protected byte[] columnQualifier;
  private OCompareOp compareOp;
  private byte[] compareValue;
  private boolean filterIfMissing = false;
  private boolean latestVersionOnly = true;

  public enum OCompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER
  }

  public OSingleColumnValueFilter(byte[] qualifier, OCompareOp compareOp, byte[] value) {
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
    this.compareValue = value;
  }

  /**
   * @return operator
   */
  public OCompareOp getOperator() {
    return compareOp;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return columnQualifier;
  }

  public byte[] getValue() {
    return compareValue;
  }

  /**
   * Get whether entire row should be filtered if column is not found.
   * @return true if row should be skipped if column not found, false if row should be let through
   *         anyways
   */
  public boolean getFilterIfMissing() {
    return filterIfMissing;
  }

  /**
   * Set whether entire row should be filtered if column is not found.
   * <p>
   * If true, the entire row will be skipped if the column is not found.
   * <p>
   * If false, the row will pass if the column is not found. This is default.
   * @param filterIfMissing flag
   */
  public void setFilterIfMissing(boolean filterIfMissing) {
    this.filterIfMissing = filterIfMissing;
  }

  /**
   * Get whether only the latest version of the column value should be compared. If true, the row
   * will be returned if only the latest version of the column value matches. If false, the row will
   * be returned if any version of the column value matches. The default is true.
   * @return return value
   */
  public boolean getLatestVersionOnly() {
    return latestVersionOnly;
  }

  /**
   * Set whether only the latest version of the column value should be compared. If true, the row
   * will be returned if only the latest version of the column value matches. If false, the row will
   * be returned if any version of the column value matches. The default is true.
   * @param latestVersionOnly flag
   */
  public void setLatestVersionOnly(boolean latestVersionOnly) {
    this.latestVersionOnly = latestVersionOnly;
  }

}

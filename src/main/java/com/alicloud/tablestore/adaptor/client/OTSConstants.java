package com.alicloud.tablestore.adaptor.client;

public class OTSConstants {
  /**
   * An empty instance.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  public static final byte[] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Last row in a table.
   */
  public static final byte[] EMPTY_END_ROW = EMPTY_START_ROW;

  public static final String PRIMARY_KEY_NAME = "__rowkey__";

  public static final String UTF8_ENCODING = "utf-8";

  public static long LATEST_TIMESTAMP = Long.MAX_VALUE;

}

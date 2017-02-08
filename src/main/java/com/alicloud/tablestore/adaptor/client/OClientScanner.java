/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Implements the result iterator interface for a scan.
 */
public class OClientScanner extends AbstractOClientScanner {
  public static final int DEFAULT_CLIENT_SCANNER_CACHING = 10;
  private final LinkedList<com.alicloud.tablestore.adaptor.struct.OResult> cache = new LinkedList<com.alicloud.tablestore.adaptor.struct.OResult>();
  private final int caching;
  private final OTSInterface ots;
  private final com.alicloud.tablestore.adaptor.struct.OScan scan;
  private boolean closed = false;
  private final String table;
  private byte[] nextRow;

  public OClientScanner(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, OTSInterface ots) throws IOException {
    this.ots = ots;
    this.scan = scan;
    this.table = tableName;
    if (scan.getCaching() > 0) {
      this.caching = scan.getCaching();
    } else {
      this.caching = DEFAULT_CLIENT_SCANNER_CACHING;
    }

    ByteArrayOutputStream nextRowOutputStream = new ByteArrayOutputStream();
    List<com.alicloud.tablestore.adaptor.struct.OResult> results = ots.scan(tableName, scan, this.caching, nextRowOutputStream);
    nextRow = nextRowOutputStream.toByteArray();
    cache.addAll(results);
    if (nextRow.length == 0) {
      this.closed = true;
    }
  }

  public com.alicloud.tablestore.adaptor.struct.OResult next() throws IOException {
    // If the scanner is closed and there's nothing left in the cache, next is a
    // no-op.
    while (true) {
      if (cache.isEmpty() && this.closed) {
        return null;
      }
      if (cache.isEmpty()) {
        if (nextRow.length != 0) {
          scan.setStartRow(nextRow);
        } else {
          this.closed = true;
          return null;
        }
        ByteArrayOutputStream nextRowOutputStream = new ByteArrayOutputStream();
        List<com.alicloud.tablestore.adaptor.struct.OResult> results = ots.scan(this.table, scan, this.caching, nextRowOutputStream);
        nextRow = nextRowOutputStream.toByteArray();
        for (com.alicloud.tablestore.adaptor.struct.OResult oRes : results) {
          cache.add(oRes);
        }
        if (nextRow.length == 0) {
          this.closed = true;
        }
      }
    if (cache.size() > 0) {
      return cache.poll();
      }
    }
  }

  public com.alicloud.tablestore.adaptor.struct.OResult[] next(int nbRows) throws IOException {
    // Collect values to be returned here
    ArrayList<com.alicloud.tablestore.adaptor.struct.OResult> resultSets = new ArrayList<com.alicloud.tablestore.adaptor.struct.OResult>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      com.alicloud.tablestore.adaptor.struct.OResult next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new com.alicloud.tablestore.adaptor.struct.OResult[resultSets.size()]);
  }

  public void close() {
    // Do Nothing
  }

}

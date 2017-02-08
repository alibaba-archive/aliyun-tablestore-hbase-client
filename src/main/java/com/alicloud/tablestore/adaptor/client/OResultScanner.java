/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for client-side scanning. Go to {@link OTSAdapter} to obtain instances.
 */
public interface OResultScanner extends Closeable, Iterable<com.alicloud.tablestore.adaptor.struct.OResult> {
  /**
   * Grab the next row's worth of values. The scanner will return a HResult.
   * @return OResult object if there is another row, null if the scanner is exhausted.
   * @throws IOException e
   */
  public com.alicloud.tablestore.adaptor.struct.OResult next() throws IOException;

  /**
   * @param nbRows number of rows to return
   * @return Between zero and <param>nbRows</param> OResults
   * @throws IOException e
   */
  public com.alicloud.tablestore.adaptor.struct.OResult[] next(int nbRows) throws IOException;

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  public void close();
}

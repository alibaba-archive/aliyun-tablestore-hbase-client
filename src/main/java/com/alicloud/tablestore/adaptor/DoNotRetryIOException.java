/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor;

import java.io.IOException;

/**
 * Subclass if exception is not meant to be retried
 */
public class DoNotRetryIOException extends IOException {
  private static final long serialVersionUID = 4154101863360220306L;

  /**
   * default constructor
   */
  public DoNotRetryIOException() {
    super();
  }

  /**
   * @param message
   */
  public DoNotRetryIOException(String message) {
    super(message);
  }

  /**
   * @param message
   * @param cause
   */
  public DoNotRetryIOException(String message, Throwable cause) {
    super(message, cause);
  }
}

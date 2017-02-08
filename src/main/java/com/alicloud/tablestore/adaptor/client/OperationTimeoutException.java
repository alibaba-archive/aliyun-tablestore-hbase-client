/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import java.io.IOException;

/**
 * Thrown if timeout executing the OTS operation
 */
public class OperationTimeoutException extends IOException {
  private static final long serialVersionUID = -655944941443633296L;

  /**
   * default constructor
   */
  public OperationTimeoutException() {
    super();
  }

  /**
   * @param message
   */
  public OperationTimeoutException(String message) {
    super(message);
  }

  /**
   * @param message
   * @param cause
   */
  public OperationTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

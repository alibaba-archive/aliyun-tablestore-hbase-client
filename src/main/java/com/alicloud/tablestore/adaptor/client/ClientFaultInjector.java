package com.alicloud.tablestore.adaptor.client;

import java.util.concurrent.atomic.AtomicLong;

public class ClientFaultInjector {

  public static ClientFaultInjector instance = new ClientFaultInjector();

  public static AtomicLong exceptionNum = new AtomicLong(0);

  public void fetchFromHBaseServiceException() {
  }
}

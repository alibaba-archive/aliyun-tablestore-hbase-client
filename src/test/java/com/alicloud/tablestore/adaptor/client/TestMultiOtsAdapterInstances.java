/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class TestMultiOtsAdapterInstances {
  @Test
  public void testMultiOtsAdapterInstances() throws IOException {
    OTSAdapter.mockMode = true;
    try {
      TablestoreClientConf conf1 = getHcsClientConf();
      TablestoreClientConf conf2 = conf1;
      OTSAdapter hcs1 = OTSAdapter.getInstance(conf1);
      OTSAdapter hcs2 = OTSAdapter.getInstance(conf2);
      // Equal with the same conf object
      assertTrue(hcs1 == hcs2);
      TablestoreClientConf conf3 = getHcsClientConf();
      OTSAdapter hcs3 = OTSAdapter.getInstance(conf3);
      // Not Equal with the different conf object
      assertTrue(hcs1 != hcs3);

      assertEquals(2, OTSAdapter.OTS_INSTANCES.size());
      assertEquals(2, hcs1.refCount);
      assertEquals(2, hcs2.refCount);
      assertEquals(1, hcs3.refCount);
      hcs1.close();
      assertEquals(1, hcs1.refCount);
      assertEquals(1, hcs2.refCount);
      hcs2.close();
      assertEquals(0, hcs1.refCount);
      assertEquals(0, hcs2.refCount);
      hcs3.close();
      assertEquals(0, hcs3.refCount);

      assertEquals(0, OTSAdapter.OTS_INSTANCES.size());
    } finally {
      OTSAdapter.mockMode = false;
    }

  }

  private TablestoreClientConf getHcsClientConf() {
    TablestoreClientConf conf = new TablestoreClientConf();
    return conf;
  }

}

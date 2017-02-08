/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.hbase.util.Bytes;

public class TestOPut {

  public static final byte[] ROW_01 = Bytes.toBytes("row-01");
  public static final byte[] QUALIFIER_01 = Bytes.toBytes("qualifier-01");
  public static final byte[] WRONG_QUALIFIER = Bytes.toBytes("qualifier-02");
  public static final byte[] VALUE_01 = Bytes.toBytes("value-01");
  public static final long TS = 1234567L;
  public OPut put = new OPut(ROW_01);

  @Before
  public void setUp() {
    put.add(QUALIFIER_01, TS, VALUE_01);
  }

  @Test
  public void testHasIgnoreValueIgnoreTS() {
    Assert.assertTrue(put.has(QUALIFIER_01));
    Assert.assertFalse(put.has(WRONG_QUALIFIER));
  }

  @Test
  public void testHasIgnoreValue() {
    Assert.assertTrue(put.has(QUALIFIER_01, TS));
    Assert.assertFalse(put.has(QUALIFIER_01, TS + 1));
  }

  @Test
  public void testHasIgnoreTS() {
    Assert.assertTrue(put.has(QUALIFIER_01, VALUE_01));
    Assert.assertFalse(put.has(VALUE_01, QUALIFIER_01));
  }

  @Test
  public void testHas() {
    Assert.assertTrue(put.has(QUALIFIER_01, TS, VALUE_01));
    // Bad TS
    Assert.assertFalse(put.has(QUALIFIER_01, TS + 1, VALUE_01));
    // Bad Value
    Assert.assertFalse(put.has(QUALIFIER_01, TS, QUALIFIER_01));
    // Bad Qual
    Assert.assertFalse(put.has(WRONG_QUALIFIER, TS, VALUE_01));
  }

}

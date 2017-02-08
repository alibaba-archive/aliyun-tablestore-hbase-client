/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import static org.junit.Assert.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestOResult {

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] value = Bytes.toBytes("value");

  private static OColumnValue[] genCVs(final byte[] value, final long timestamp, final int cols) {
    OColumnValue[] cvs = new OColumnValue[cols];

    for (int i = 0; i < cols; i++) {
      cvs[i] =
          new OColumnValue(row, Bytes.toBytes(i), timestamp,
                  OColumnValue.Type.PUT, Bytes.add(value, Bytes.toBytes(i)));
    }
    return cvs;
  }

  @Test
  public void testBasic() throws Exception {
    OColumnValue[] cvs = genCVs(value, 1, 100);

    Arrays.sort(cvs, OColumnValue.KEY_COMPARATOR);

    OResult r = new OResult(cvs);

    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<OColumnValue> ks = r.getColumn(qf);
      assertEquals(1, ks.size());
      assertByteEquals(qf, ks.get(0).getQualifier());

      assertEquals(ks.get(0), r.getColumnLatest(qf));
      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(qf));
      assertTrue(r.containsColumn(qf));
    }
  }

  @Test
  public void testMultiVersion() throws Exception {
    OColumnValue[] cvs1 = genCVs(value, 1, 100);
    OColumnValue[] cvs2 = genCVs(value, 200, 100);

    OColumnValue[] cvs = new OColumnValue[cvs1.length + cvs2.length];
    System.arraycopy(cvs1, 0, cvs, 0, cvs1.length);
    System.arraycopy(cvs2, 0, cvs, cvs1.length, cvs2.length);

    Arrays.sort(cvs, OColumnValue.KEY_COMPARATOR);

    OResult r = new OResult(cvs);
    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<OColumnValue> ks = r.getColumn(qf);
      assertEquals(2, ks.size());
      assertByteEquals(qf, ks.get(0).getQualifier());
      assertEquals(200, ks.get(0).getTimestamp());

      assertEquals(ks.get(0), r.getColumnLatest(qf));
      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(qf));
      assertTrue(r.containsColumn(qf));
    }
  }

  @Test
  public void testCompareResults() throws Exception {
    OColumnValue[] cvs1 = genCVs(value, 1, 100);
    OColumnValue[] cvs2 = genCVs(value, 1, 100);
    Arrays.sort(cvs1, OColumnValue.KEY_COMPARATOR);
    Arrays.sort(cvs2, OColumnValue.KEY_COMPARATOR);
    OResult res1 = new OResult(cvs1);
    OResult res2 = new OResult(cvs2);
    OResult.compareResults(res1, res2);
  }

  private static void assertByteEquals(byte[] expected, byte[] actual) {
    if (Bytes.compareTo(expected, actual) != 0) {
      fail("expected:<" + Bytes.toString(expected) + "> but was:<" + Bytes.toString(actual) + ">");
    }
  }

}

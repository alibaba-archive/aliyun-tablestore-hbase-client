/**
 * Copyright 2014 Aliyun.com All right reserved. This software is the confidential and proprietary
 * information of Aliyun.com ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the license agreement you
 * entered into with Aliyun.com
 */
package com.alicloud.tablestore.adaptor.struct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.tablestore.adaptor.client.OTSConstants;

public class TestODelete {
  public static final byte[] ROW_01 = Bytes.toBytes("row-01");
  public static final byte[] QUALIFIER_01 = Bytes.toBytes("qualifier-01");
  public static final long TS = 1234567L;

  @Test
  public void testToOTSParameter() {
    // delete row
    ODelete delete = new ODelete(ROW_01);
    RowDeleteChange rowDeleteChange = (RowDeleteChange) delete.toOTSParameter("T");
    assertEquals("T", rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));

    // delete columns
    delete = new ODelete(ROW_01);
    delete.deleteColumns(QUALIFIER_01);
    RowUpdateChange rowUpdateChange = (RowUpdateChange) delete.toOTSParameter("T");
    assertEquals("T", rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));
    assertEquals(1, rowUpdateChange.getColumnsToUpdate().size());
    assertEquals(Bytes.toString(QUALIFIER_01), rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getName());
    assertEquals(RowUpdateChange.Type.DELETE_ALL, rowUpdateChange.getColumnsToUpdate().get(0).getSecond());

    // delete column
    delete = new ODelete(ROW_01);
    delete.deleteColumn(QUALIFIER_01, TS);
    rowUpdateChange = (RowUpdateChange) delete.toOTSParameter("T");
    assertEquals("T", rowDeleteChange.getTableName());
    assertEquals(1, rowDeleteChange.getPrimaryKey().size());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW_01, rowDeleteChange.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));
    assertEquals(1, rowUpdateChange.getColumnsToUpdate().size());
    assertEquals(Bytes.toString(QUALIFIER_01), rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getName());
    assertEquals(TS, rowUpdateChange.getColumnsToUpdate().get(0).getFirst().getTimestamp());
    assertEquals(RowUpdateChange.Type.DELETE, rowUpdateChange.getColumnsToUpdate().get(0).getSecond());

  }

}

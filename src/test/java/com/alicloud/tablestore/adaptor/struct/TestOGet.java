package com.alicloud.tablestore.adaptor.struct;

import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.struct.OGet;
import com.alicloud.tablestore.adaptor.client.OTSConstants;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import org.junit.Test;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOGet {

  static final String tableName = "T";
  static final byte[] ROW = Bytes.toBytes("Row");
  static final byte[] QUALIFIER = Bytes.toBytes("Qualifier");

  @Test
  public void testToOTSParameter() throws IOException {

    OGet get = new OGet(ROW);
    SingleRowQueryCriteria criteria = get.toOTSParameter(tableName);
    assertEquals(1, criteria.getMaxVersions());
    assertEquals(0, criteria.getTimeRange().getStart());
    assertEquals(Long.MAX_VALUE, criteria.getTimeRange().getEnd());
    assertEquals(tableName, criteria.getTableName());
    assertEquals(OTSConstants.PRIMARY_KEY_NAME,
            criteria.getPrimaryKey().getPrimaryKeyColumn(0).getName());
    assertTrue(Bytes.equals(ROW,
            criteria.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary()));

    get = new OGet(ROW);
    get.setMaxVersions(3);
    get.setTimeRange(10, 100);
    OSingleColumnValueFilter filter = new OSingleColumnValueFilter(QUALIFIER,
            OSingleColumnValueFilter.OCompareOp.EQUAL, new byte[0]);
    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(true);
    get.setFilter(filter);
    criteria = get.toOTSParameter(tableName);
    assertEquals(3, criteria.getMaxVersions());
    assertEquals(10, criteria.getTimeRange().getStart());
    assertEquals(100, criteria.getTimeRange().getEnd());
    assertEquals(Bytes.toString(QUALIFIER), ((SingleColumnValueFilter) criteria.getFilter()).getColumnName());
    assertEquals(SingleColumnValueFilter.CompareOperator.EQUAL,
            ((SingleColumnValueFilter)criteria.getFilter()).getOperator());
    assertEquals(0, ((SingleColumnValueFilter) criteria.getFilter()).getColumnValue().asBinary().length);
    assertEquals(false, ((SingleColumnValueFilter)criteria.getFilter()).isPassIfMissing());
    assertEquals(true, ((SingleColumnValueFilter)criteria.getFilter()).isLatestVersionsOnly());

  }
}

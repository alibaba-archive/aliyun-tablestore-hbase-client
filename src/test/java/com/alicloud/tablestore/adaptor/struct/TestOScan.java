package com.alicloud.tablestore.adaptor.struct;

import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestOScan {

  private  static final String tableName = "T";
  private static final byte[] ROW = Bytes.toBytes("Row");
  private static final byte[] QUALIFIER = Bytes.toBytes("Qualifier");

  @Test
  public void testToOTSParameter() throws IOException {

    OScan scan = new OScan();
    RangeRowQueryCriteria criteria = scan.toOTSParameter(tableName);
    assertEquals(1, criteria.getMaxVersions());
    assertEquals(0, criteria.getTimeRange().getStart());
    assertEquals(Long.MAX_VALUE, criteria.getTimeRange().getEnd());
    assertEquals(tableName, criteria.getTableName());

    scan = new OScan();
    scan.setMaxVersions(3);
    scan.setTimeRange(10, 100);
    OSingleColumnValueFilter filter = new OSingleColumnValueFilter(QUALIFIER,
            OSingleColumnValueFilter.OCompareOp.EQUAL, new byte[0]);
    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(true);
    scan.setFilter(filter);
    criteria = scan.toOTSParameter(tableName);
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

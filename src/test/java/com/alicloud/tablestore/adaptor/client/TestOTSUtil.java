package com.alicloud.tablestore.adaptor.client;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.FilterType;
import com.alicloud.tablestore.adaptor.client.util.Bytes;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;
import com.alicloud.tablestore.adaptor.filter.OFilter;
import com.alicloud.tablestore.adaptor.filter.OFilterList;
import com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter;
import com.alicloud.tablestore.adaptor.struct.OColumnValue;
import com.alicloud.tablestore.adaptor.struct.OResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;

public class TestOTSUtil {
  @Test
  public void testParseOTSRowToResult() {
    List<PrimaryKeyColumn> primaryKeyColumnList = new ArrayList<PrimaryKeyColumn>();
    primaryKeyColumnList.add(new PrimaryKeyColumn(OTSConstants.PRIMARY_KEY_NAME,
            PrimaryKeyValue.fromBinary(Bytes.toBytes("pk"))));
    PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumnList);

    int columnNum = 10;
    int versionNum = 10;
    List<Column> columns = new ArrayList<Column>();
    for (int col = 0; col < columnNum; col++) {
      for (int ver = 0; ver < versionNum; ver++) {
        columns.add(new Column("col" + col, ColumnValue.fromBinary(Bytes.toBytes("value")), ver));
      }
    }

    Row row = new Row(primaryKey, columns);
    OResult result = OTSUtil.parseOTSRowToResult(row);

    assertEquals(100, result.raw().length);

    List<OColumnValue> keyValues = result.getColumn(Bytes.toBytes("col0"));
    assertEquals(10, keyValues.size());
    assertEquals("value", Bytes.toString(keyValues.get(0).getValue()));

    OColumnValue keyValue = result.getColumnLatest(Bytes.toBytes("col8"));
    assertEquals(9, keyValue.getTimestamp());

    assertEquals(false, result.containsColumn(Bytes.toBytes("col10")));
    assertEquals(true, result.containsColumn(Bytes.toBytes("col1")));

    NavigableMap<byte[], NavigableMap<Long, byte[]>> map = result.getMap();

    assertEquals(10, map.size());
    assertEquals("value", Bytes.toString(map.get(Bytes.toBytes("col0")).get(9L)));
    assertEquals("value", Bytes.toString(result.getValue(Bytes.toBytes("col0"))));

    NavigableMap<byte[], byte[]> noVerMap = result.getNoVersionMap();

    assertEquals(10, noVerMap.size());
    assertEquals("value", Bytes.toString(noVerMap.get(Bytes.toBytes("col0"))));
  }

  @Test
  public void testToFilter() {
    OFilter filter =
            new OSingleColumnValueFilter(Bytes.toBytes("col"),
                    OSingleColumnValueFilter.OCompareOp.EQUAL, Bytes.toBytes("value"));
    com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter otsFilter
            = (com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter) OTSUtil.toFilter(filter);

    assertEquals("col", otsFilter.getColumnName());
    assertEquals(com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.EQUAL, otsFilter.getOperator());
    assertEquals("value", Bytes.toString(otsFilter.getColumnValue().asBinary()));
    assertEquals(FilterType.SINGLE_COLUMN_VALUE_FILTER, otsFilter.getFilterType());

    filter = new OFilterList();
    CompositeColumnValueFilter otsFilter1 = (CompositeColumnValueFilter) OTSUtil.toFilter(filter);
    assertEquals(CompositeColumnValueFilter.LogicOperator.AND, otsFilter1.getOperationType());

    OFilterList filterList = new OFilterList(OFilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(new OSingleColumnValueFilter(Bytes.toBytes("col1"),
            OSingleColumnValueFilter.OCompareOp.GREATER, Bytes.toBytes("val")));
    filterList.addFilter(new OSingleColumnValueFilter(Bytes.toBytes("col2"),
            OSingleColumnValueFilter.OCompareOp.LESS, Bytes.toBytes("val")));
    CompositeColumnValueFilter otsFilter2 = (CompositeColumnValueFilter) OTSUtil.toFilter(filterList);

    assertEquals(FilterType.COMPOSITE_COLUMN_VALUE_FILTER, otsFilter2.getFilterType());
    assertEquals(CompositeColumnValueFilter.LogicOperator.OR, otsFilter2.getOperationType());
    assertEquals(2, otsFilter2.getSubFilters().size());
    assertEquals(FilterType.SINGLE_COLUMN_VALUE_FILTER, otsFilter2.getSubFilters().get(0).getFilterType());

    com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter otsFilter3 =
            (com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter) otsFilter2.getSubFilters().get(0);
    assertEquals("col1", otsFilter3.getColumnName());
    assertEquals("val", Bytes.toString(otsFilter3.getColumnValue().asBinary()));
    assertEquals(com.alicloud.openservices.tablestore.model.filter.
            SingleColumnValueFilter.CompareOperator.GREATER_THAN, otsFilter3.getOperator());
  }

}

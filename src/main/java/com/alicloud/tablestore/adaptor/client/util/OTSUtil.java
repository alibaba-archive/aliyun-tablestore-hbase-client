package com.alicloud.tablestore.adaptor.client.util;

import com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.TimeRange;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Row;

public class OTSUtil {

  public static PrimaryKey toPrimaryKey(byte[] rowKey, String rowKeyName) {
    PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
    primaryKeyColumns[0] = new PrimaryKeyColumn(rowKeyName, PrimaryKeyValue.fromBinary(rowKey));
    return new PrimaryKey(primaryKeyColumns);
  }

  public static TimeRange toTimeRange(com.alicloud.tablestore.adaptor.struct.OTimeRange timeRange) {
    return new TimeRange(timeRange.getMin(), timeRange.getMax());
  }

  private static SingleColumnValueFilter.CompareOperator toCompareOperator(
      com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter.OCompareOp compareOp) {
    switch (compareOp) {
    case LESS:
      return SingleColumnValueFilter.CompareOperator.LESS_THAN;
    case LESS_OR_EQUAL:
      return SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
    case EQUAL:
      return SingleColumnValueFilter.CompareOperator.EQUAL;
    case GREATER_OR_EQUAL:
      return SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
    case GREATER:
      return SingleColumnValueFilter.CompareOperator.GREATER_THAN;
    case NOT_EQUAL:
      return SingleColumnValueFilter.CompareOperator.NOT_EQUAL;
    default:
      return null;
    }
  }

  public static ColumnValueFilter toFilter(com.alicloud.tablestore.adaptor.filter.OFilter filter) {
    Preconditions.checkNotNull(filter);
    if (filter instanceof com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) {
      String columnName = Bytes.toString(((com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) filter).getQualifier());
      SingleColumnValueFilter.CompareOperator compareOperator =
          toCompareOperator(((com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) filter).getOperator());
      ColumnValue columnValue =
          ColumnValue.fromBinary(((com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) filter).getValue());
      SingleColumnValueFilter singleColumnValueFilter =
              new SingleColumnValueFilter(columnName, compareOperator, columnValue);
      // passIfMissing = !filterIfMissing
      singleColumnValueFilter.setPassIfMissing(!((com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) filter).getFilterIfMissing());
      singleColumnValueFilter.setLatestVersionsOnly(((com.alicloud.tablestore.adaptor.filter.OSingleColumnValueFilter) filter).getLatestVersionOnly());
      return singleColumnValueFilter;
    } else if (filter instanceof com.alicloud.tablestore.adaptor.filter.OFilterList) {
      CompositeColumnValueFilter.LogicOperator logicOperator = null;
      switch (((com.alicloud.tablestore.adaptor.filter.OFilterList) filter).getOperator()) {
        case MUST_PASS_ALL:
          logicOperator = CompositeColumnValueFilter.LogicOperator.AND;
          break;
        case MUST_PASS_ONE:
          logicOperator = CompositeColumnValueFilter.LogicOperator.OR;
      }
      CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(logicOperator);
      for (com.alicloud.tablestore.adaptor.filter.OFilter filterItem : ((com.alicloud.tablestore.adaptor.filter.OFilterList) filter).getFilters()) {
        compositeFilter.addFilter(toFilter(filterItem));
      }
      return compositeFilter;
    } else {
      throw new UnsupportedOperationException("Unsupported filter type.");
    }
  }

  public static com.alicloud.tablestore.adaptor.struct.OResult parseOTSRowToResult(Row row) {
    if (row == null) {
      return new com.alicloud.tablestore.adaptor.struct.OResult(new com.alicloud.tablestore.adaptor.struct.OColumnValue[0]);
    }
    int columnNum = row.getColumns().length;
    com.alicloud.tablestore.adaptor.struct.OColumnValue[] kvs = new com.alicloud.tablestore.adaptor.struct.OColumnValue[columnNum];
    for (int i = 0; i < columnNum; i++) {
      kvs[i] =
          new com.alicloud.tablestore.adaptor.struct.OColumnValue(row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asBinary(),
              Bytes.toBytes(row.getColumns()[i].getName()), row.getColumns()[i].getTimestamp(),
              com.alicloud.tablestore.adaptor.struct.OColumnValue.Type.PUT, row.getColumns()[i].getValue().asBinary());
    }
    return new com.alicloud.tablestore.adaptor.struct.OResult(kvs);
  }
}

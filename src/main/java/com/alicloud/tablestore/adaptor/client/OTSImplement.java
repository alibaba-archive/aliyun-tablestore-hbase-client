package com.alicloud.tablestore.adaptor.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.tablestore.adaptor.DoNotRetryIOException;
import com.alicloud.tablestore.adaptor.client.util.OTSUtil;
import com.alicloud.tablestore.adaptor.client.util.Preconditions;
import com.alicloud.tablestore.adaptor.struct.OTableDescriptor;
import org.apache.hadoop.hbase.HConstants;

class OTSImplement implements OTSInterface {

  private int retryCount;
  private int maxBatchRowCount;
  private int maxBatchDataSize;
  private int maxScanLimit;
  private AsyncClientInterface ots = null;

  OTSImplement(TablestoreClientConf conf) {
    retryCount = conf.getRetryCount();
    maxBatchRowCount = conf.getOTSMaxBatchRowCount();
    maxBatchDataSize = conf.getOTSMaxBatchDataSize();
    maxScanLimit = conf.getOTSMaxScanLimit();
    ClientConfiguration otsConf = new ClientConfiguration();
    otsConf.setMaxConnections(conf.getOTSMaxConnections());
    otsConf.setSocketTimeoutInMillisecond(conf.getOTSSocketTimeout());
    otsConf.setConnectionTimeoutInMillisecond(conf.getOTSConnectionTimeout());
    otsConf.setRetryStrategy(new DefaultRetryStrategy());
    ots =
        new AsyncClient(conf.getOTSEndpoint(), conf.getTablestoreAccessKeyId(), conf.getTablestoreAccessKeySecret(),
            conf.getOTSInstanceName(), otsConf);
  }

  public com.alicloud.tablestore.adaptor.struct.OResult get(String tableName, com.alicloud.tablestore.adaptor.struct.OGet get) throws IOException {
    try {
      SingleRowQueryCriteria singleRowQueryCriteria = get.toOTSParameter(tableName);
      GetRowRequest getRowRequest = new GetRowRequest(singleRowQueryCriteria);
      GetRowResponse result = ots.getRow(getRowRequest, null).get();
      return OTSUtil.parseOTSRowToResult(result.getRow());
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public List<com.alicloud.tablestore.adaptor.struct.OResult> getMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OGet> gets) throws IOException {
    Object[] r1 = new Object[gets.size()];
    batchGet(tableName, gets, r1);
    List<com.alicloud.tablestore.adaptor.struct.OResult> results = new ArrayList<com.alicloud.tablestore.adaptor.struct.OResult>();
    for (Object o : r1) {
      results.add((com.alicloud.tablestore.adaptor.struct.OResult) o);
    }
    return results;
  }

  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit) throws IOException {
    return scan(tableName, scan, limit, new ByteArrayOutputStream());
  }

  public List<com.alicloud.tablestore.adaptor.struct.OResult> scan(String tableName, com.alicloud.tablestore.adaptor.struct.OScan scan, int limit, ByteArrayOutputStream nextRow) throws IOException {
    Preconditions.checkArgument(limit <= maxScanLimit,
            String.format("The limit:%d can not exceed %d.", limit, maxScanLimit));
    Preconditions.checkNotNull(nextRow);
    RangeRowQueryCriteria criteria = scan.toOTSParameter(tableName);
    if (criteria.getLimit() == -1 || limit < criteria.getLimit()) {
      criteria.setLimit(limit);
    }

    GetRangeResponse getRangeResult = null;
    try {
      getRangeResult = ots.getRange(new GetRangeRequest(criteria), null).get();
    } catch (Throwable ex) {
      throw new IOException(ex);
    }

    List<com.alicloud.tablestore.adaptor.struct.OResult> results = new ArrayList<com.alicloud.tablestore.adaptor.struct.OResult>();
    for (Row row : getRangeResult.getRows()) {
      results.add(OTSUtil.parseOTSRowToResult(row));
    }
    if (!results.isEmpty()) {
      results.get(results.size() - 1).setOtsResult(getRangeResult);
    }
    nextRow.reset();
    if (getRangeResult.getNextStartPrimaryKey() != null) {
      nextRow.write(getRangeResult.getNextStartPrimaryKey()
              .getPrimaryKeyColumn(com.alicloud.tablestore.adaptor.client.OTSConstants.PRIMARY_KEY_NAME).getValue().asBinary());
    }
    return results;
  }

  public void put(String tableName, com.alicloud.tablestore.adaptor.struct.OPut put) throws IOException {
    try {
      RowUpdateChange rowPutChange = put.toOTSParameter(tableName);
      ots.updateRow(new UpdateRowRequest(rowPutChange), null).get();
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public void putMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.OPut> puts) throws IOException {
    Object[] r1 = new Object[puts.size()];
    batchPut(tableName, puts, r1);
  }

  public void delete(String tableName, com.alicloud.tablestore.adaptor.struct.ODelete delete) throws IOException {
    try {
      RowChange rowChange = delete.toOTSParameter(tableName);
      if (rowChange instanceof RowDeleteChange) {
        ots.deleteRow(new DeleteRowRequest((RowDeleteChange) rowChange), null).get();
      } else if (rowChange instanceof RowUpdateChange) {
        ots.updateRow(new UpdateRowRequest((RowUpdateChange) rowChange), null).get();
      }
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public void deleteMultiple(String tableName, List<com.alicloud.tablestore.adaptor.struct.ODelete> deletes) throws IOException {
    final List<com.alicloud.tablestore.adaptor.struct.ODelete> deleteRows = new ArrayList<com.alicloud.tablestore.adaptor.struct.ODelete>();
    final List<com.alicloud.tablestore.adaptor.struct.ODelete> deleteColumns = new ArrayList<com.alicloud.tablestore.adaptor.struct.ODelete>();
    for (com.alicloud.tablestore.adaptor.struct.ODelete delete : deletes) {
      if (delete.isEmpty()) {
        deleteRows.add(delete);
      } else {
        deleteColumns.add(delete);
      }
    }
    Object[] r1 = new Object[deleteRows.size()];
    Object[] r2 = new Object[deleteColumns.size()];
    Throwable error = null;
    try {
      batchDeleteRow(tableName, deleteRows, r1);
    } catch (Throwable ex) {
      error = ex;
    }
    try {
      batchDeleteColumn(tableName, deleteColumns, r2);
    } catch (Throwable ex) {
      error = ex;
    }
    if (error != null) {
      throw new DoNotRetryIOException(error.getMessage(), error);
    }
  }

  public void update(String tableName, com.alicloud.tablestore.adaptor.struct.OUpdate update) throws IOException {
    try {
      RowUpdateChange rowUpdateChange = update.toOTSParameter(tableName);
      ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  private boolean shouldRetry(Throwable ex) {
    if (ex instanceof TableStoreException) {
      String errorCode = ((TableStoreException) ex).getErrorCode();
      if (errorCode.equals(OTSErrorCode.INVALID_PARAMETER)
          || errorCode.equals(OTSErrorCode.AUTHORIZATION_FAILURE)
          || errorCode.equals(OTSErrorCode.INVALID_PK)
          || errorCode.equals(OTSErrorCode.OUT_OF_COLUMN_COUNT_LIMIT)
          || errorCode.equals(OTSErrorCode.OUT_OF_ROW_SIZE_LIMIT)
          || errorCode.equals(OTSErrorCode.CONDITION_CHECK_FAIL)
          || errorCode.equals(OTSErrorCode.REQUEST_TOO_LARGE)) {
        return false;
      }
    }
    return true;
  }

  private void batchGet(String tableName, List<com.alicloud.tablestore.adaptor.struct.OGet> gets, Object[] results) throws IOException {
    int size = gets.size();
    if (size == 0) return;

    int retried = 0;
    while (true) {
      Throwable error = null;

      Future[] futures = new Future[size];

      for (int i = 0; i < size; i++) {

        // check should retry
        boolean shouldRetryVar = false;
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            shouldRetryVar = true;
          } else {
            error = (Throwable) results[i];
          }
        }

        if (retried == 0 || shouldRetryVar) {
          SingleRowQueryCriteria criteria = gets.get(i).toOTSParameter(tableName);
          try {
            Future<GetRowResponse> future = ots.getRow(new GetRowRequest(criteria), null);
            futures[i] = future;
          } catch (Throwable ex) {
            results[i] = ex;
            error = ex;
          }
        }
      }

      for (int i = 0; i < size; i++) {
        if (futures[i] != null) {
          try {
            GetRowResponse result = (GetRowResponse) futures[i].get();
            results[i] = OTSUtil.parseOTSRowToResult(result.getRow());
          } catch (Throwable ex) {
            results[i] = ex;
            error = ex;
          }
        }
      }

      if (error == null) return;

      boolean hasItemNeedRetry = false;
      for (int i = 0; i < size; i++) {
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            hasItemNeedRetry = true;
            break;
          }
        }
      }

      if (retried >= retryCount || !hasItemNeedRetry) {
        throw new DoNotRetryIOException(error.getMessage(), error);
      }

      try {
        Thread.sleep(10 + retried * 1000); // 与RetryInvocationHandler中一致。
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      retried++;
    }
  }

  private void batchPut(String tableName, List<com.alicloud.tablestore.adaptor.struct.OPut> puts, Object[] results) throws IOException {
    int size = puts.size();
    if (size == 0) return;

    // check puts
    for (int i = 0; i < size; i++) {
      if (puts.get(i).size() == 0) {
        results[i] = new IllegalArgumentException("The put cannot be empty.");
      }
    }

    int retried = 0;
    while (true) {
      Throwable error = null;

      List<BatchWriteRowRequest> batches = new ArrayList<BatchWriteRowRequest>();
      List<List<Integer>> batchIndexes = new ArrayList<List<Integer>>();

      BatchWriteRowRequest request = new BatchWriteRowRequest();
      List<Integer> requestIndexes = new ArrayList<Integer>();
      int batchCount = 0;
      long batchSize = 0;
      for (int i = 0; i < size; i++) {

        // check should retry
        boolean shouldRetryVar = false;
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            shouldRetryVar = true;
          } else {
            error = (Throwable) results[i];
          }
        }
        if (retried == 0 || shouldRetryVar) {
          if (batchCount < maxBatchRowCount
              && (batchCount == 0 || (batchSize + puts.get(i).getWritableSize()) <= maxBatchDataSize)) {
            request.addRowChange(puts.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchSize += puts.get(i).getWritableSize();
            batchCount++;
          } else {
            batches.add(request);
            batchIndexes.add(requestIndexes);
            request = new BatchWriteRowRequest();
            requestIndexes = new ArrayList<Integer>();
            request.addRowChange(puts.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchCount = 1;
            batchSize = puts.get(i).getWritableSize();
          }
        }
        if (i == size - 1 && batchCount > 0) {
          batches.add(request);
          batchIndexes.add(requestIndexes);
        }
      }

      for (int i = 0; i < batches.size(); i++) {
        try {
          Future<BatchWriteRowResponse> future = ots.batchWriteRow(batches.get(i), null);
          try {
            BatchWriteRowResponse result = (BatchWriteRowResponse) future.get();
            for (BatchWriteRowResponse.RowResult res : result.getSucceedRows()) {
              results[batchIndexes.get(i).get(res.getIndex())] = new com.alicloud.tablestore.adaptor.struct.OResult(new com.alicloud.tablestore.adaptor.struct.OColumnValue[0]);
            }
            for (BatchWriteRowResponse.RowResult res : result.getFailedRows()) {
              TableStoreException ex =
                  new TableStoreException(res.getError().getMessage(), null, res.getError().getCode(), result.getRequestId(),
                      0);
              results[batchIndexes.get(i).get(res.getIndex())] = ex;
              error = ex;
            }
          } catch (Throwable ex) {
            error = ex;
            if (!shouldRetry(ex)) {
              throw ex;
            }
            for (int idx : batchIndexes.get(i)) {
              results[idx] = ex;
            }
          }
        } catch (Throwable ex) {
          error = ex;
          if (!shouldRetry(ex)) {
            throw new DoNotRetryIOException(error.getMessage(), error);
          }
          for (int idx : batchIndexes.get(i)) {
            results[idx] = ex;
          }
        }
      }

      if (error == null) return;

      boolean hasItemNeedRetry = false;
      for (int i = 0; i < size; i++) {
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            hasItemNeedRetry = true;
            break;
          }
        }
      }

      if (retried >= retryCount || !hasItemNeedRetry) {
        throw new DoNotRetryIOException(error.getMessage(), error);
      }

      try {
        Thread.sleep(10 + retried * 1000); // 与RetryInvocationHandler中一致。
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      retried++;
    }
  }

  private void batchDeleteColumn(String tableName, List<com.alicloud.tablestore.adaptor.struct.ODelete> deletes, Object[] results)
      throws IOException {
    int size = deletes.size();
    if (size == 0) return;

    int retried = 0;
    while (true) {
      Throwable error = null;

      List<BatchWriteRowRequest> batches = new ArrayList<BatchWriteRowRequest>();
      List<List<Integer>> batchIndexes = new ArrayList<List<Integer>>();

      BatchWriteRowRequest request = new BatchWriteRowRequest();
      List<Integer> requestIndexes = new ArrayList<Integer>();
      int batchCount = 0;
      long batchSize = 0;
      for (int i = 0; i < size; i++) {

        // check should retry
        boolean shouldRetryVar = false;
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            shouldRetryVar = true;
          } else {
            error = (Throwable) results[i];
          }
        }
        if (retried == 0 || shouldRetryVar) {
          if (batchCount < maxBatchRowCount
              && (batchCount == 0 || (batchSize + deletes.get(i).getWritableSize()) <= maxBatchDataSize)) {
            request.addRowChange(deletes.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchSize += deletes.get(i).getWritableSize();
            batchCount++;
          } else {
            batches.add(request);
            batchIndexes.add(requestIndexes);
            request = new BatchWriteRowRequest();
            requestIndexes = new ArrayList<Integer>();
            request.addRowChange(deletes.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchCount = 1;
            batchSize = deletes.get(i).getWritableSize();
          }
        }
        if (i == size - 1 && batchCount > 0) {
          batches.add(request);
          batchIndexes.add(requestIndexes);
        }
      }

      Future[] futures = new Future[batches.size()];
      for (int i = 0; i < batches.size(); i++) {
        try {
          futures[i] = ots.batchWriteRow(batches.get(i), null);
        } catch (Throwable ex) {
          error = ex;
          if (!shouldRetry(ex)) {
            throw new DoNotRetryIOException(error.getMessage(), error);
          }
          for (int idx : batchIndexes.get(i)) {
            results[idx] = ex;
          }
        }
      }

      for (int i = 0; i < batches.size(); i++) {
        if (futures[i] != null) {
          try {
            BatchWriteRowResponse result = (BatchWriteRowResponse) futures[i].get();
            for (BatchWriteRowResponse.RowResult res : result.getSucceedRows()) {
              results[batchIndexes.get(i).get(res.getIndex())] = new com.alicloud.tablestore.adaptor.struct.OResult(new com.alicloud.tablestore.adaptor.struct.OColumnValue[0]);
            }
            for (BatchWriteRowResponse.RowResult res : result.getFailedRows()) {
              TableStoreException ex =
                  new TableStoreException(res.getError().getMessage(), null, res.getError().getCode(), result.getRequestId(),
                      0);
              results[batchIndexes.get(i).get(res.getIndex())] = ex;
              error = ex;
            }
          } catch (Throwable ex) {
            error = ex;
            if (!shouldRetry(ex)) {
              throw new DoNotRetryIOException(error.getMessage(), error);
            }
            for (int idx : batchIndexes.get(i)) {
              results[idx] = ex;
            }
          }
        }
      }

      if (error == null) return;

      boolean hasItemNeedRetry = false;
      for (int i = 0; i < size; i++) {
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            hasItemNeedRetry = true;
            break;
          }
        }
      }

      if (retried >= retryCount || !hasItemNeedRetry) {
        throw new DoNotRetryIOException(error.getMessage(), error);
      }

      try {
        Thread.sleep(10 + retried * 1000); // 与RetryInvocationHandler中一致。
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      retried++;
    }
  }

  private void batchDeleteRow(String tableName, List<com.alicloud.tablestore.adaptor.struct.ODelete> deletes, Object[] results)
      throws IOException {
    int size = deletes.size();
    if (size == 0) return;

    int retried = 0;
    while (true) {
      Throwable error = null;

      List<BatchWriteRowRequest> batches = new ArrayList<BatchWriteRowRequest>();
      List<List<Integer>> batchIndexes = new ArrayList<List<Integer>>();

      BatchWriteRowRequest request = new BatchWriteRowRequest();
      List<Integer> requestIndexes = new ArrayList<Integer>();
      int batchCount = 0;
      long batchSize = 0;
      for (int i = 0; i < size; i++) {

        // check should retry
        boolean shouldRetryVar = false;
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            shouldRetryVar = true;
          } else {
            error = (Throwable) results[i];
          }
        }
        if (retried == 0 || shouldRetryVar) {
          if (batchCount < maxBatchRowCount
              && (batchCount == 0 || (batchSize + deletes.get(i).getWritableSize()) <= maxBatchDataSize)) {
            request.addRowChange(deletes.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchSize += deletes.get(i).getWritableSize();
            batchCount++;
          } else {
            batches.add(request);
            batchIndexes.add(requestIndexes);
            request = new BatchWriteRowRequest();
            requestIndexes = new ArrayList<Integer>();
            request.addRowChange(deletes.get(i).toOTSParameter(tableName));
            requestIndexes.add(i);
            batchCount = 1;
            batchSize = deletes.get(i).getWritableSize();
          }
        }
        if (i == size - 1 && batchCount > 0) {
          batches.add(request);
          batchIndexes.add(requestIndexes);
        }
      }

      Future[] futures = new Future[batches.size()];
      for (int i = 0; i < batches.size(); i++) {
        try {
          futures[i] = ots.batchWriteRow(batches.get(i), null);
        } catch (Throwable ex) {
          error = ex;
          if (!shouldRetry(ex)) {
            throw new DoNotRetryIOException(error.getMessage(), error);
          }
          for (int idx : batchIndexes.get(i)) {
            results[idx] = ex;
          }
        }
      }

      for (int i = 0; i < batches.size(); i++) {
        if (futures[i] != null) {
          try {
            BatchWriteRowResponse result = (BatchWriteRowResponse) futures[i].get();
            for (BatchWriteRowResponse.RowResult res : result.getSucceedRows()) {
              results[batchIndexes.get(i).get(res.getIndex())] = new com.alicloud.tablestore.adaptor.struct.OResult(new com.alicloud.tablestore.adaptor.struct.OColumnValue[0]);
            }
            for (BatchWriteRowResponse.RowResult res : result.getFailedRows()) {
              TableStoreException ex =
                  new TableStoreException(res.getError().getMessage(), null, res.getError().getCode(), result.getRequestId(),
                      0);
              results[batchIndexes.get(i).get(res.getIndex())] = ex;
              error = ex;
            }
          } catch (Throwable ex) {
            error = ex;
            if (!shouldRetry(ex)) {
              throw new DoNotRetryIOException(error.getMessage(), error);
            }
            for (int idx : batchIndexes.get(i)) {
              results[idx] = ex;
            }
          }
        }
      }

      if (error == null) return;

      boolean hasItemNeedRetry = false;
      for (int i = 0; i < size; i++) {
        if (results[i] instanceof Throwable) {
          if (shouldRetry((Throwable) results[i])) {
            hasItemNeedRetry = true;
            break;
          }
        }
      }

      if (retried >= retryCount || !hasItemNeedRetry) {
        throw new DoNotRetryIOException(error.getMessage(), error);
      }

      try {
        Thread.sleep(10 + retried * 1000); // 与RetryInvocationHandler中一致。
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      retried++;
    }
  }

  public void batch(final String tableName, List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions, Object[] results)
      throws IOException {

    int size = actions.size();
    Throwable error = null;

    int startIdx = 0;
    int lastType = 0;

    for (int i = 0; i <= size; i++) {

      int type = 0;
      if (i < size) {
        if (actions.get(i) instanceof com.alicloud.tablestore.adaptor.struct.OGet) {
          type = 1;
        }
        if (actions.get(i) instanceof com.alicloud.tablestore.adaptor.struct.OPut) {
          type = 2;
        }
        if (actions.get(i) instanceof com.alicloud.tablestore.adaptor.struct.ODelete) {
          if (((com.alicloud.tablestore.adaptor.struct.ODelete) actions.get(i)).isEmpty()) {
            type = 3;
          } else {
            type = 4;
          }
        }
      }

      boolean shouldCommit = false;
      if (i < size) {
        if (type == 0) {
          results[i] = new UnsupportedOperationException("The action type is unsupported.");
          shouldCommit = true;
        } else {
          if (lastType != type) {
            shouldCommit = true;
          }
        }
      } else {
        shouldCommit = true;
      }

      if (shouldCommit) {
        if (lastType != 0) {
          Object[] res = new Object[i - startIdx];
          try {
            if (lastType == 1) {
              batchGet(tableName, (List<com.alicloud.tablestore.adaptor.struct.OGet>) actions.subList(startIdx, i), res);
            }
            if (lastType == 2) {
              batchPut(tableName, (List<com.alicloud.tablestore.adaptor.struct.OPut>) actions.subList(startIdx, i), res);
            }
            if (lastType == 3) {
              batchDeleteRow(tableName, (List<com.alicloud.tablestore.adaptor.struct.ODelete>) actions.subList(startIdx, i), res);
            }
            if (lastType == 4) {
              batchDeleteColumn(tableName, (List<com.alicloud.tablestore.adaptor.struct.ODelete>) actions.subList(startIdx, i), res);
            }
          } catch (Throwable ex) {
            error = ex;
          }
          for (int idx = startIdx; idx < i; idx++) {
            results[idx] = res[idx - startIdx];
          }
        }
        startIdx = i;
        lastType = type;
      }
    }
    if (error != null) {
      throw new DoNotRetryIOException(error.getMessage(), error);
    }
  }

  public Object[] batch(String tableName, List<? extends com.alicloud.tablestore.adaptor.struct.ORow> actions) throws IOException {
    Object[] results = new Object[actions.size()];
    batch(tableName, actions, results);
    return results;
  }

  public void close() throws IOException {
    ots.shutdown();
  }

  public List<String> listTable() throws IOException {
    try {
      ListTableResponse response = ots.listTable(null).get();
      return response.getTableNames();
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public void createTable(OTableDescriptor descriptor) throws IOException {
    TableMeta tableMeta = new TableMeta(descriptor.getTableName());
    tableMeta.addPrimaryKeyColumn(OTSConstants.PRIMARY_KEY_NAME, PrimaryKeyType.BINARY);

    TableOptions tableOptions = new TableOptions();
    tableOptions.setMaxVersions(descriptor.getMaxVersion());
    tableOptions.setTimeToLive(descriptor.getTimeToLive());
    tableOptions.setMaxTimeDeviation(descriptor.getMaxTimeDeviation());

    CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
    try {
      ots.createTable(request, null).get();
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public void deleteTable(String tableName) throws IOException {
    DeleteTableRequest request = new DeleteTableRequest(tableName);
    try {
      ots.deleteTable(request, null);
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public OTableDescriptor describeTable(String tableName) throws IOException {
    DescribeTableRequest request = new DescribeTableRequest(tableName);
    try {
      DescribeTableResponse response = ots.describeTable(request, null).get();

      OTableDescriptor tableDescriptor = new OTableDescriptor(tableName);
      tableDescriptor.setMaxTimeDeviation(response.getTableOptions().getMaxTimeDeviation());
      tableDescriptor.setMaxVersion(response.getTableOptions().getMaxVersions());
      tableDescriptor.setTimeToLive(response.getTableOptions().getTimeToLive());

      List<PrimaryKey> primmaryKeys = response.getShardSplits();
      byte[] start = HConstants.EMPTY_START_ROW;
      for (PrimaryKey primaryKey : primmaryKeys) {
        byte[] end = primaryKey.getPrimaryKeyColumn(0).getValue().asBinary();
        tableDescriptor.addSplitKey(start, end);
        start = end;
      }
      tableDescriptor.addSplitKey(start, HConstants.EMPTY_END_ROW);
      return tableDescriptor;
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }

  public void updateTable(OTableDescriptor descriptor) throws IOException {
    TableOptions tableOptions = new TableOptions();
    tableOptions.setMaxVersions(descriptor.getMaxVersion());
    tableOptions.setTimeToLive(descriptor.getTimeToLive());
    tableOptions.setMaxTimeDeviation(descriptor.getMaxTimeDeviation());

    UpdateTableRequest request = new UpdateTableRequest(descriptor.getTableName());
    request.setTableOptionsForUpdate(tableOptions);
    try {
      ots.updateTable(request, null).get();
    } catch (Throwable ex) {
      if (shouldRetry(ex)) {
        throw new IOException(ex);
      } else {
        throw new DoNotRetryIOException(ex.getMessage(), ex);
      }
    }
  }
}

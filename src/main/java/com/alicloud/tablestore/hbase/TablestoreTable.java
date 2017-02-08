package com.alicloud.tablestore.hbase;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.tablestore.adaptor.client.OResultScanner;
import com.alicloud.tablestore.adaptor.client.OTSAdapter;
import com.alicloud.tablestore.adaptor.struct.*;
import com.google.protobuf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.util.*;

public class TablestoreTable implements Table {

    private final TableName tableName;
    private final String tableNameStr;
    private TablestoreConnection connection;
    private OTSAdapter tablestoreAdaptor;

    private volatile long writeBufferSize;
    private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
    private volatile boolean clearBufferOnFail;
    private volatile boolean autoFlush;
    private long currentWriteBufferSize;
    private int maxKeyValueSize;

    private ColumnMapping tablestoreColumnMapping;

    private int scannerCaching;
    private int maxLimit;

    public TablestoreTable(TablestoreConnection connection, TableName tableName) {
        this.tableName = tableName;
        this.tableNameStr = tableName.getNameAsString();
        this.connection = connection;
        this.tablestoreAdaptor = OTSAdapter.getInstance(this.connection.getTablestoreConf());
        this.writeBufferSize = this.connection.getConfiguration().getLong("hbase.client.write.buffer", 2097152);
        this.maxKeyValueSize = this.connection.getConfiguration().getInt("hbase.client.keyvalue.maxsize", -1);
        this.clearBufferOnFail = true;
        this.autoFlush = true;
        this.currentWriteBufferSize = 0;
        this.tablestoreColumnMapping = new ColumnMapping(tableName.getNameAsString(), this.connection.getConfiguration());
        this.scannerCaching = this.connection.getConfiguration().getInt(
                HConstants.HBASE_CLIENT_SCANNER_CACHING,
                HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
        this.maxLimit = 5000;
    }


    public int getOperationTimeout() {
        return this.tablestoreAdaptor.getOperationTimeout();
    }

    public Result append(Append append) throws IOException {
        throw new UnsupportedOperationException("append");
    }

    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        Object[] results = new Object[actions.size()];
        batch(actions, results);
        return results;
    }

    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("batchCallback");
    }

    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("batchCallback");
    }

    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        List<ORow> tactions = new ArrayList<ORow>(actions.size());
        for (Row action : actions) {
            if (action instanceof Get) {
                tactions.add(ElementConvertor.toOtsGet((Get) action, this.tablestoreColumnMapping));
            } else if (action instanceof Put) {
                tactions.add(ElementConvertor.toOtsPut((Put) action, this.tablestoreColumnMapping));
            } else if (action instanceof Delete) {
                tactions.add(ElementConvertor.toOtsDelete((Delete) action,this.tablestoreColumnMapping));
            } else {
                throw new UnsupportedOperationException("Unsupport type "
                        + action.getClass().getName() + " in batch operation.");
            }
        }
        Object[] tresults = new Object[results.length];
        try {
            this.tablestoreAdaptor.batch(tableNameStr, tactions, tresults);
        } finally {
            for (int i = 0; i < tresults.length; i++) {
                if (tresults[i] == null) {
                    results[i] = null;
                } else if (tresults[i] instanceof OResult) {
                    results[i] = ElementConvertor
                            .toHBaseResult((OResult) tresults[i], this.tablestoreColumnMapping);
                } else if (tresults[i] instanceof Throwable) {
                    results[i] = tresults[i];
                } else {
                    throw new IOException("Get unsupported result type " + tresults[i]);
                }
            }
        }
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                  byte[] value, Delete delete) throws IOException {
        return checkAndDelete(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, delete);
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
        if (!Arrays.equals(delete.getRow(), row)) {
            throw new UnsupportedOperationException("CheckAndDelete does not support check one row but delete other row");
        }


        ODelete odelete = ElementConvertor.toOtsDelete(delete, this.tablestoreColumnMapping);
        Condition condition = ElementConvertor.toOtsCondition(family, qualifier, compareOp, value, this.tablestoreColumnMapping);
        odelete.setCondition(condition);

        try {
            this.tablestoreAdaptor.delete(tableNameStr, odelete);
        } catch (IOException ex) {
            if (ex.getCause().getCause() instanceof TableStoreException) {
                TableStoreException exception = (TableStoreException)ex.getCause().getCause();
                if (exception.getErrorCode().equals("OTSConditionCheckFail")) {
                    return false;
                }
            }
            throw ex;
        }
        return true;
    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                               byte[] value, Put put) throws IOException {
        return checkAndPut(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, put);
    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
        if (!Arrays.equals(put.getRow(), row)) {
            throw new UnsupportedOperationException("CheckAndPut does not support check one row but put other row");
        }

        OPut oput = ElementConvertor.toOtsPut(put, this.tablestoreColumnMapping);
        Condition condition = ElementConvertor.toOtsCondition(family, qualifier, compareOp, value, this.tablestoreColumnMapping);
        oput.setCondition(condition);

        try {
            this.tablestoreAdaptor.put(tableNameStr, oput);
        } catch (IOException ex) {
            if (ex.getCause().getCause() instanceof TableStoreException) {
                TableStoreException exception = (TableStoreException)ex.getCause().getCause();
                if (exception.getErrorCode().equals("OTSConditionCheckFail")) {
                    return false;
                }
            }
            throw ex;
        }
        return true;
    }

    public void close() throws IOException {
        if (this.tablestoreAdaptor != null) {
            this.tablestoreAdaptor.close();
            this.tablestoreAdaptor = null;
        }
    }

    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new UnsupportedOperationException();
    }

    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
        throw new UnsupportedOperationException("coprocessorService");
    }

    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
        throw new UnsupportedOperationException("coprocessorService");
    }

    public void delete(Delete delete) throws IOException {
        this.tablestoreAdaptor.delete(tableNameStr,
                ElementConvertor.toOtsDelete(delete,this.tablestoreColumnMapping));
    }

    public void delete(List<Delete> deletes) throws IOException {
        this.tablestoreAdaptor.deleteMultiple(tableNameStr,
                ElementConvertor.toOtsDeleteList(deletes,this.tablestoreColumnMapping));
    }

    public boolean exists(Get get) throws IOException {
        Result result = get(get);
        return result.getRow() != null;
    }

    public boolean[] existsAll(List<Get> gets) throws IOException {
        Result[] results = get(gets);
        boolean[] existResult = new boolean[results.length];
        for (int i = 0; i < results.length; i++) {
            existResult[i] = (results[i].getRow() != null);
        }
        return existResult;
    }

    public Result get(Get get) throws IOException {
        OResult result = this.tablestoreAdaptor.get(tableNameStr,
                ElementConvertor.toOtsGet(get, this.tablestoreColumnMapping));
        return ElementConvertor.toHBaseResult(result, this.tablestoreColumnMapping);
    }

    public Result[] get(List<Get> gets) throws IOException {
        List<OResult> results = this.tablestoreAdaptor.getMultiple(tableNameStr,
                ElementConvertor.toOtsGets(gets, this.tablestoreColumnMapping));
        return ElementConvertor.toHBaseResults(results, this.tablestoreColumnMapping);
    }

    public Configuration getConfiguration() {
        return this.connection.getConfiguration();
    }

    public ResultScanner getScanner(Scan scan) throws IOException {
        Preconditions.checkNotNull(scan);

        if (scan.getCaching() == -1) {
            scan.setCaching(this.maxLimit);
        }
        OScan oscan = ElementConvertor.toOtsScan(scan,
                this.tablestoreColumnMapping);
        OResultScanner oscanner = this.tablestoreAdaptor.getScanner(tableNameStr, oscan);
        return new Scanner(oscanner, this.tablestoreColumnMapping);
    }

    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    public HTableDescriptor getTableDescriptor() throws IOException {
        OTableDescriptor oTableDescriptor = this.tablestoreAdaptor.describeTable(this.tableName.getNameAsString());
        return ElementConvertor.toHbaseTableDescriptor(oTableDescriptor, this.tablestoreColumnMapping);
    }

    public TableName getName() {
        return tableName;
    }

    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public Result increment(Increment increment) throws IOException {
        throw new UnsupportedOperationException("increment");
    }

    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
                                     long amount) throws IOException {
        throw new UnsupportedOperationException("incrementColumnValue");
    }

    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        throw new UnsupportedOperationException("incrementColumnValue");
    }

    public void mutateRow(RowMutations rm) throws IOException {
        OUpdate oupdate = ElementConvertor.toOtsUpdate(rm, this.tablestoreColumnMapping);
        this.tablestoreAdaptor.update(tableNameStr, oupdate);
    }

    public void put(Put put) throws IOException {
        if (this.autoFlush) {
            OPut oput = ElementConvertor.toOtsPut(put, this.tablestoreColumnMapping);
            this.tablestoreAdaptor.put(tableNameStr, oput);
        } else {
            doPut(Collections.singletonList(put));
        }
    }

    public void put(List<Put> puts) throws IOException {
        if (this.autoFlush) {
            this.tablestoreAdaptor.putMultiple(tableNameStr,
                    ElementConvertor.toOtsPuts(puts, this.tablestoreColumnMapping));
        } else {
            doPut(puts);
        }
    }

    private void doPut(final List<Put> puts) throws IOException {
        List<OPut> flushPuts = null;
        synchronized (writeBuffer) {
            for (Put put : puts) {
                validatePut(put);
                writeBuffer.add(put);
                currentWriteBufferSize += put.heapSize();
            }
            if (autoFlush || currentWriteBufferSize > writeBufferSize) {
                flushPuts = ElementConvertor.toOtsPuts(writeBuffer, this.tablestoreColumnMapping);
                writeBuffer.clear();
                currentWriteBufferSize = 0;
            }
        }
        if (flushPuts != null && !flushPuts.isEmpty()) {
            doCommits(flushPuts);
        }
    }

    private void doCommits(final List<OPut> puts) throws IOException {
        boolean flushSuccessfully = false;
        try {
            this.tablestoreAdaptor.putMultiple(tableNameStr, puts);
            flushSuccessfully = true;
        } finally {
            if (!flushSuccessfully && !clearBufferOnFail) {
                List<Put> hputs = ElementConvertor.toHBasePuts(puts, this.tablestoreColumnMapping);
                synchronized (writeBuffer) {
                    for (Put put : hputs) {
                        writeBuffer.add(put);
                        currentWriteBufferSize += put.heapSize();
                    }
                }
            }
        }
    }

    // validate for well-formedness
    private void validatePut(final Put put) throws IllegalArgumentException {
        if (put.isEmpty()) {
            throw new IllegalArgumentException("No columns to insert");
        }
        if (maxKeyValueSize > 0) {
            for (List<KeyValue> list : put.getFamilyMap().values()) {
                for (KeyValue kv : list) {
                    if (kv.getLength() > maxKeyValueSize) {
                        throw new IllegalArgumentException("KeyValue size too large");
                    }
                }
            }
        }
    }

    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        this.writeBufferSize = writeBufferSize;
    }

    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback) throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
        if (!Arrays.equals(mutation.getRow(), row)) {
            throw new UnsupportedOperationException("CheckAndMutation does not support check one row but Mutate other row");
        }

        OUpdate oupdate = ElementConvertor.toOtsUpdate(mutation, this.tablestoreColumnMapping);
        Condition condition = ElementConvertor.toOtsCondition(family, qualifier, compareOp, value, this.tablestoreColumnMapping);
        oupdate.setCondition(condition);

        try {
            this.tablestoreAdaptor.update(tableNameStr, oupdate);
        } catch (IOException ex) {
            if (ex.getCause().getCause() instanceof TableStoreException) {
                TableStoreException exception = (TableStoreException)ex.getCause().getCause();
                if (exception.getErrorCode().equals("OTSConditionCheckFail")) {
                    return false;
                }
            }
            throw ex;
        }
        return true;
    }

    private static class Scanner extends AbstractClientScanner {
        private final OResultScanner tscanner;
        private ColumnMapping tablestoreColumnMapping;

        /**
         * @param scanner
         */
        public Scanner(OResultScanner scanner, ColumnMapping otsColumnMappingStrategy) {
            this.tscanner = scanner;
            this.tablestoreColumnMapping = otsColumnMappingStrategy;
        }

        public void close() {
            this.tscanner.close();
        }

        @Override
        public Result next() throws IOException {
            OResult r = tscanner.next();
            if (r == null) {
                return null;
            }
            return ElementConvertor.toHBaseResult(r, this.tablestoreColumnMapping);
        }

        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

        @Override
        public boolean renewLease() {
            throw new UnsupportedOperationException();
        }
    }

    public HConnection getConnection() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the number of rows that a scanner will fetch at once.
     * <p>
     * The default value comes from {@code hbase.client.scanner.caching}.
     *
     * @deprecated Use {@link Scan#setCaching(int)} and {@link Scan#getCaching()}
     */
    public int getScannerCaching() {
        return this.scannerCaching;
    }

    /**
     * do nothing, this method is used by code{AdvancedHTableFactory}
     *
     * @param scannerCaching the number of rows a scanner will fetch at once.
     * @deprecated Use {@link Scan#setCaching(int)}
     */
    public void setScannerCaching(int scannerCaching) {
        this.scannerCaching = scannerCaching;
    }

    /**
     * do nothing, this method is used by code{AdvancedHTableFactory}
     * @param operationTimeout
     */
    public void setOperationTimeout(int operationTimeout) {
        this.tablestoreAdaptor.setOperationTimeout(operationTimeout);
    }
}

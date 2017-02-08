package com.alicloud.tablestore.hbase;


import com.alicloud.tablestore.adaptor.client.OTSAdapter;
import com.alicloud.tablestore.adaptor.struct.ODelete;
import com.alicloud.tablestore.adaptor.struct.OPut;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TablestoreBufferedMutator implements BufferedMutator {
    private final TableName tableName;
    private TablestoreConnection connection;
    private volatile long writeBufferSize;
    private long currentWriteBufferSize;
    private final ConcurrentLinkedQueue<Mutation> writeBuffer;
    private ColumnMapping columnMapping;
    private OTSAdapter adapter;
    private volatile boolean clearBufferOnFail;

    public TablestoreBufferedMutator(TablestoreConnection connection, TableName tableName) {
        this.tableName = tableName;
        this.connection = connection;
        writeBuffer = new ConcurrentLinkedQueue<Mutation>();
        this.writeBufferSize = this.connection.getConfiguration().getLong("hbase.client.write.buffer", 2097152);
        this.currentWriteBufferSize = 0;
        this.columnMapping = new ColumnMapping(tableName.getNameAsString(), this.connection.getConfiguration());
        this.adapter = OTSAdapter.getInstance(this.connection.getTablestoreConf());
        this.clearBufferOnFail = true;
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    @Override
    public void mutate(Mutation mutation) throws IOException {
        mutate(Collections.singletonList(mutation));
    }

    @Override
    public void mutate(List<? extends Mutation> list) throws IOException {
        List<OPut> flushPuts = new ArrayList<OPut>();
        List<ODelete> flushDeletes = new ArrayList<ODelete>();

        for (Mutation mutation : list) {
            writeBuffer.add(mutation);
            currentWriteBufferSize += mutation.heapSize();
        }
        if (currentWriteBufferSize >= writeBufferSize) {
            extractOMutation(flushPuts, flushDeletes);
        }

        flush(flushPuts, flushDeletes);
    }

    private void extractOMutation(List<OPut> flushPuts, List<ODelete> flushDeletes) {
        for (Mutation mutation : writeBuffer) {
            if (mutation instanceof Put) {
                flushPuts.add(ElementConvertor.toOtsPut((Put)mutation, this.columnMapping));
            } else if (mutation instanceof Delete) {
                flushDeletes.add(ElementConvertor.toOtsDelete((Delete)mutation, this.columnMapping));
            }
        }
        writeBuffer.clear();
        currentWriteBufferSize = 0;
    }

    private void flush(List<OPut> flushPuts, List<ODelete> flushDeletes) throws IOException {
        if (!flushPuts.isEmpty()) {
            commitPuts(flushPuts);
        }
        if (!flushDeletes.isEmpty()) {
            commitDeletes(flushDeletes);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.adapter != null) {
            this.adapter.close();
            this.adapter = null;
        }
    }

    @Override
    public void flush() throws IOException {
        List<OPut> flushPuts = new ArrayList<OPut>();
        List<ODelete> flushDeletes = new ArrayList<ODelete>();

        synchronized (writeBuffer) {
            extractOMutation(flushPuts, flushDeletes);
        }
        flush(flushPuts, flushDeletes);
    }

    @Override
    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setClearBufferOnFail(boolean clearBufferOnFail) {
        this.clearBufferOnFail = clearBufferOnFail;
    }

    private void commitPuts(final List<OPut> puts) throws IOException {
        boolean flushSuccessfully = false;
        try {
            this.adapter.putMultiple(tableName.getNameAsString(), puts);
            flushSuccessfully = true;
        } finally {
            if (!flushSuccessfully && !clearBufferOnFail) {
                List<Put> hputs = ElementConvertor.toHBasePuts(puts, this.columnMapping);
                synchronized (writeBuffer) {
                    for (Put put : hputs) {
                        writeBuffer.add(put);
                        currentWriteBufferSize += put.heapSize();
                    }
                }
            }
        }
    }

    private void commitDeletes(final List<ODelete> deletes) throws IOException {
        boolean flushSuccessfully = false;
        try {
            this.adapter.deleteMultiple(tableName.getNameAsString(), deletes);
            flushSuccessfully = true;
        } finally {
            if (!flushSuccessfully && !clearBufferOnFail) {
                List<Delete> hDeletes = ElementConvertor.toHBaseDeletes(deletes, this.columnMapping);
                synchronized (writeBuffer) {
                    for (Delete delete : hDeletes) {
                        writeBuffer.add(delete);
                        currentWriteBufferSize += delete.heapSize();
                    }
                }
            }
        }
    }
}

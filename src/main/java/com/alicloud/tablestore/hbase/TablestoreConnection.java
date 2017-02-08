package com.alicloud.tablestore.hbase;

import com.alicloud.tablestore.adaptor.client.TablestoreClientConf;
import com.alicloud.tablestore.adaptor.client.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CopyOnWriteArraySet;

public class TablestoreConnection implements Connection {
    private static Map<Configuration, TablestoreClientConf> globalTablestoreConfs = new HashMap<Configuration, TablestoreClientConf>();

    private final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<RegionLocator>();
    private final Configuration hbaseConf;
    private TablestoreClientConf tablestoreConf;
    private volatile boolean closed = false;
    private volatile boolean aborted;
    private volatile ExecutorService batchPool = null;

    public TablestoreConnection(Configuration conf) throws IOException {
        this(conf, false, null, null);
    }

    public TablestoreConnection(Configuration conf, boolean managed, ExecutorService pool, User user) {
        if (managed) {
            throw new UnsupportedOperationException("Tablestore does not support managed connections.");
        }

        this.hbaseConf = conf;
        this.batchPool = pool;
        this.closed = false;
        this.tablestoreConf = loadOtsConf(conf);
    }


    public Configuration getConfiguration() {
        return this.hbaseConf;
    }

    public TablestoreClientConf getTablestoreConf() {
        return tablestoreConf;
    }

    public Table getTable(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);
        return new TablestoreTable(this, tableName);
    }

    public Admin getAdmin() throws IOException {
        return new TablestoreAdmin(this);
    }

    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return getTable(tableName);
    }

    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);
        return new TablestoreBufferedMutator(this, tableName);
    }

    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        Preconditions.checkNotNull(params);
        TableName tableName = params.getTableName();
        return new TablestoreBufferedMutator(this, tableName);
    }

    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);
        return new TablestoreRegionLocator(this, tableName);
    }

    public void close() throws IOException {
        this.closed = true;
    }

    public boolean isClosed() {
        return this.closed;
    }

    public void abort(String why, Throwable e) {
        this.aborted = true;
        try {
            close();
        } catch(IOException ex) {
            throw new RuntimeException("Could not close the connection", ex);
        }
    }

    public boolean isAborted() {
        return this.aborted;
    }

    private TablestoreClientConf loadOtsConf(Configuration conf) {
        synchronized (globalTablestoreConfs) {
            TablestoreClientConf existedTablestoreConf = globalTablestoreConfs.get(conf);
            if (existedTablestoreConf == null) {
                existedTablestoreConf = new TablestoreClientConf();
                List<String> tablestoreConfKeys = new ArrayList<String>();
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_ENDPOINT);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_ACCESSID);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_ACCESSKEY);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_INSTANCENAME);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_MAX_CONNECTIONS);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_SOCKET_TIMEOUT);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_CONNECTION_TIMEOUT);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_OPERATION_TIMEOUT);
                 tablestoreConfKeys.add(TablestoreClientConf.TABLESTORE_CLIENT_RETRIES);
                // Load settings
                for (String key :  tablestoreConfKeys) {
                    if (conf.get(key) != null) {
                        existedTablestoreConf.setValue(key, conf.get(key));
                    }
                }
                globalTablestoreConfs.put(conf, existedTablestoreConf);
            }
            return existedTablestoreConf;
        }
    }
}

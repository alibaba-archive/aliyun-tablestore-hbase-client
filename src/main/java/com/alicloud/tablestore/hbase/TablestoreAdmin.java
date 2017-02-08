package com.alicloud.tablestore.hbase;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.tablestore.adaptor.client.OTSAdapter;
import com.alicloud.tablestore.adaptor.client.TablestoreClientConf;
import com.alicloud.tablestore.adaptor.struct.OTableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.util.Pair;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;

public class TablestoreAdmin implements Admin {
    private static final Map<Configuration, TablestoreClientConf> globalTablestoreConfs = new HashMap<Configuration, TablestoreClientConf>();

    private final Set<TableName> disabledTables;
    private final TablestoreConnection connection;
    private OTSAdapter tablestoreAdaptor;

    public TablestoreAdmin(TablestoreConnection connection) {
        TablestoreClientConf tablestoreConf = connection.getTablestoreConf();
        this.disabledTables = new CopyOnWriteArraySet<TableName>();
        this.tablestoreAdaptor = OTSAdapter.getInstance(tablestoreConf);
        this.connection = connection;
    }

    public int getOperationTimeout() {
        throw new UnsupportedOperationException("getOperationTimeout");
    }

    public void abort(String why, Throwable e) {
        throw new UnsupportedOperationException("abort");
    }

    public boolean isAborted() {
        throw new UnsupportedOperationException("isAborted");
    }

    public Connection getConnection() {
        return this.connection;
    }

    public boolean tableExists(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);
        return this.tablestoreAdaptor.listTable().contains(tableName.getNameAsString());
    }

    public HTableDescriptor[] listTables() throws IOException {
        List<String> tables = this.tablestoreAdaptor.listTable();
        HTableDescriptor[] tableDescriptors = new HTableDescriptor[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            tableDescriptors[i] = getTableDescriptor(TableName.valueOf(tables.get(i)));
        }
        return tableDescriptors;
    }

    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
        Preconditions.checkNotNull(pattern);

        List<String> tables = this.tablestoreAdaptor.listTable();
        List<HTableDescriptor> tableDescriptors = new ArrayList<HTableDescriptor>();
        for (int i = 0; i < tables.size(); i++) {
            if (pattern.matcher(tables.get(i)).matches()) {
                tableDescriptors.add(getTableDescriptor(TableName.valueOf(tables.get(i))));
            }
        }

        HTableDescriptor[] hTableDescriptors = new HTableDescriptor[tableDescriptors.size()];
        return tableDescriptors.toArray(hTableDescriptors);
    }

    public HTableDescriptor[] listTables(String regex) throws IOException {
        Pattern pattern = Pattern.compile(regex);
        return listTables(pattern);
    }

    public HTableDescriptor[] listTables(Pattern pattern, boolean includeSysTables) throws IOException {
        Preconditions.checkNotNull(pattern);

        return listTables(pattern);
    }

    public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
        return listTables(regex);
    }

    public TableName[] listTableNames() throws IOException {
        List<String> tables = this.tablestoreAdaptor.listTable();
        TableName[] tableNames = new TableName[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            tableNames[i] = TableName.valueOf(tables.get(i));
        }
        return tableNames;
    }

    public TableName[] listTableNames(Pattern pattern) throws IOException {
        Preconditions.checkNotNull(pattern);

        List<String> tables = this.tablestoreAdaptor.listTable();
        List<TableName> tableNames = new ArrayList<TableName>();
        for (int i = 0; i < tables.size(); i++) {
            if (pattern.matcher(tables.get(i)).matches()) {
                tableNames.add(TableName.valueOf(tables.get(i)));
            }
        }
        TableName[] tableNames1 = new TableName[tableNames.size()];
        return tableNames.toArray(tableNames1);
    }

    public TableName[] listTableNames(String regex) throws IOException {
        Pattern pattern = Pattern.compile(regex);
        return listTableNames(pattern);
    }

    public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
        Preconditions.checkNotNull(pattern);

        return listTableNames(pattern);
    }

    public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
        return listTableNames(regex);
    }

    public HTableDescriptor getTableDescriptor(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        OTableDescriptor oTableDescriptor =  this.tablestoreAdaptor.describeTable(tableName.getNameAsString());

        ColumnMapping columnMapping = new ColumnMapping(tableName.getNameAsString(), this.connection.getConfiguration());
        return ElementConvertor.toHbaseTableDescriptor(oTableDescriptor, columnMapping);
    }

    public void createTable(HTableDescriptor desc) throws IOException {
        Preconditions.checkNotNull(desc);

        Set<byte[]> familiesKeys = desc.getFamiliesKeys();
        if (familiesKeys.size() > 1) {
            throw new UnsupportedOperationException("Only support one family");
        }

        int maxVersion = 1;
        int ttl = Integer.MAX_VALUE;
        if (familiesKeys.size() == 1) {
            HColumnDescriptor descriptor = desc.getFamily(familiesKeys.iterator().next());
            if (descriptor.getMaxVersions() > 0) {
                maxVersion = descriptor.getMaxVersions();
            }

            ttl = descriptor.getTimeToLive();
        }
        OTableDescriptor tableDescriptor = new OTableDescriptor(desc.getNameAsString(), maxVersion, ttl);

        this.tablestoreAdaptor.createTable(tableDescriptor);
    }

    public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
        throw new UnsupportedOperationException("createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)");
    }

    public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
        throw new UnsupportedOperationException("createTable(HTableDescriptor desc, byte[][] splitKeys)");
    }

    public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
        throw new UnsupportedOperationException("createTableAsync(HTableDescriptor desc, byte[][] splitKeys)");
    }

    public void deleteTable(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        this.tablestoreAdaptor.deleteTable(tableName.getNameAsString());
    }

    public HTableDescriptor[] deleteTables(String regex) throws IOException {
        HTableDescriptor[] tables = listTables(regex);
        for (HTableDescriptor tableName : tables) {
            deleteTable(tableName.getTableName());
        }
        return tables;
    }

    public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
        Preconditions.checkNotNull(pattern);

        HTableDescriptor[] tables = listTables(pattern);
        for (HTableDescriptor tableName : tables) {
            deleteTable(tableName.getTableName());
        }
        return tables;
    }

    public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
        Preconditions.checkNotNull(tableName);

        HTableDescriptor descriptor = getTableDescriptor(tableName);
        deleteTable(descriptor.getTableName());
        createTable(descriptor);
    }

    public void enableTable(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        if (!this.tableExists(tableName)) {
            throw new TableNotFoundException(tableName);
        }
        disabledTables.remove(tableName);
    }

    public void enableTableAsync(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        this.enableTable(tableName);
    }

    public HTableDescriptor[] enableTables(String regex) throws IOException {
        HTableDescriptor[] tableDescriptors = this.listTables(regex);
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            this.enableTable(tableDescriptor.getTableName());
        }
        return tableDescriptors;
    }

    public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
        Preconditions.checkNotNull(pattern);

        HTableDescriptor[] tableDescriptors = this.listTables(pattern);
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            this.enableTable(tableDescriptor.getTableName());
        }
        return tableDescriptors;
    }

    public void disableTableAsync(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        disableTable(tableName);
    }

    public void disableTable(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        if (!this.tableExists(tableName)) {
            throw new TableNotFoundException(tableName);
        }
        if (this.isTableDisabled(tableName)) {
            throw new TableNotEnabledException(tableName);
        }
        disabledTables.add(tableName);
    }

    public HTableDescriptor[] disableTables(String regex) throws IOException {
        HTableDescriptor[] tableDescriptors = this.listTables(regex);
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            this.disableTable(tableDescriptor.getTableName());
        }
        return tableDescriptors;
    }

    public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
        Preconditions.checkNotNull(pattern);

        HTableDescriptor[] tableDescriptors = this.listTables(pattern);
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            this.disableTable(tableDescriptor.getTableName());
        }
        return tableDescriptors;
    }

    public boolean isTableEnabled(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        return !isTableDisabled(tableName);
    }

    public boolean isTableDisabled(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        return disabledTables.contains(tableName);
    }

    public boolean isTableAvailable(TableName tableName) throws IOException {
        Preconditions.checkNotNull(tableName);

        return tableExists(tableName);
    }

    public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
        Preconditions.checkNotNull(tableName);

        return isTableAvailable(tableName);
    }

    public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
        return new Pair<Integer, Integer>(0, 0);
    }

    public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
        return new Pair<Integer, Integer>(0, 0);
    }

    public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
        throw new UnsupportedOperationException("addColumn");
    }

    public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
        throw new UnsupportedOperationException("deleteColumn");
    }

    public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {
        Preconditions.checkNotNull(tableName);

        int maxVersion = descriptor.getMaxVersions();
        int ttl = descriptor.getTimeToLive();
        OTableDescriptor tableDescriptor = new OTableDescriptor(tableName.getNameAsString(), maxVersion, ttl);

        this.tablestoreAdaptor.updateTable(tableDescriptor);
    }

    public void closeRegion(String regionname, String serverName) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    public void closeRegion(byte[] regionname, String serverName) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
        throw new UnsupportedOperationException("closeRegion");
    }

    public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
        throw new UnsupportedOperationException("getOnlineRegions");
    }

    public void flush(TableName tableName) throws IOException {
        Log.info("flush is a no-op");
    }

    public void flushRegion(byte[] regionName) throws IOException {
        Log.info("flushRegion is a no-op");
    }

    public void compact(TableName tableName) throws IOException {
        Log.info("compact is a no-op");
    }

    public void compactRegion(byte[] regionName) throws IOException {
        Log.info("compactRegion is a no-op");
    }

    public void compact(TableName tableName, byte[] columnFamily) throws IOException {
        Log.info("compact is a no-op");
    }

    public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
        Log.info("compactRegion is a no-op");
    }

    public void majorCompact(TableName tableName) throws IOException {
        Log.info("majorCompact is a no-op");
    }

    public void majorCompactRegion(byte[] regionName) throws IOException {
        Log.info("majorCompactRegion is a no-op");
    }

    public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {
        Log.info("majorCompact is a no-op");
    }

    public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
        Log.info("majorCompactRegion is a no-op");
    }

    public void compactRegionServer(ServerName sn, boolean major) throws IOException, InterruptedException {
        Log.info("compactRegionServer is a no-op");
    }

    public void move(byte[] encodedRegionName, byte[] destServerName) throws IOException {
        Log.info("move is a no-op");
    }

    public void assign(byte[] regionName) throws IOException {
        Log.info("assign is a no-op");
    }

    public void unassign(byte[] regionName, boolean force) throws IOException {
        Log.info("unassign is a no-op");
    }

    public void offline(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("offline");
    }

    public boolean setBalancerRunning(boolean on, boolean synchronous) throws IOException {
        return true;
    }

    public boolean balancer() throws IOException {
        Log.info("balancer is a no-op");
        return true;
    }

    public boolean isBalancerEnabled() throws IOException {
        return false;
    }

    public boolean normalize() throws IOException {
        throw new UnsupportedOperationException("normalize");
    }

    public boolean isNormalizerEnabled() throws IOException {
        return false;
    }

    public boolean setNormalizerRunning(boolean on) throws IOException {
        throw new UnsupportedOperationException("setNormalizerRunning");
    }

    public boolean enableCatalogJanitor(boolean enable) throws IOException {
        throw new UnsupportedOperationException("enableCatalogJanitor");
    }

    public int runCatalogScan() throws IOException {
        throw new UnsupportedOperationException("runCatalogScan");
    }

    public boolean isCatalogJanitorEnabled() throws IOException {
        throw new UnsupportedOperationException("isCatalogJanitorEnabled");
    }

    public void mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB, boolean forcible) throws IOException {
        Log.info("mergeRegions is a no-op");
    }

    public void split(TableName tableName) throws IOException {
        Log.info("split is a no-op");
    }

    public void splitRegion(byte[] regionName) throws IOException {
        Log.info("splitRegion is a no-op");
    }

    public void split(TableName tableName, byte[] splitPoint) throws IOException {
        Log.info("split is a no-op");
    }

    public void splitRegion(byte[] regionName, byte[] splitPoint) throws IOException {
        Log.info("splitRegion is a no-op");
    }

    public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {
        throw new UnsupportedOperationException("modifyTable");
    }

    public void shutdown() throws IOException {
        throw new UnsupportedOperationException("shutdown");
    }

    public void stopMaster() throws IOException {
        throw new UnsupportedOperationException("stopMaster");
    }

    public void stopRegionServer(String hostnamePort) throws IOException {
        throw new UnsupportedOperationException("stopRegionServer");
    }

    public ClusterStatus getClusterStatus() throws IOException {
        throw new UnsupportedOperationException("getClusterStatus");
    }

    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
        throw new UnsupportedOperationException("createNamespace");
    }

    public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
        throw new UnsupportedOperationException("modifyNamespace");
    }

    public void deleteNamespace(String name) throws IOException {
        throw new UnsupportedOperationException("deleteNamespace");
    }

    public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
        throw new UnsupportedOperationException("getNamespaceDescriptor");
    }

    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        throw new UnsupportedOperationException("listNamespaceDescriptors");
    }

    public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
        throw new UnsupportedOperationException("listTableDescriptorsByNamespace");
    }

    public TableName[] listTableNamesByNamespace(String name) throws IOException {
        throw new UnsupportedOperationException("listTableNamesByNamespace");
    }

    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("getTableRegions");
    }

    public void close() throws IOException {
        if (this.tablestoreAdaptor != null) {
            this.tablestoreAdaptor.close();
            this.tablestoreAdaptor = null;
        }
    }

    public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames) throws IOException {
        Preconditions.checkNotNull(tableNames);

        HTableDescriptor[] tableDescriptors = new HTableDescriptor[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            Preconditions.checkNotNull(tableName);
            tableDescriptors[i] = getTableDescriptor(tableName);
        }
        return tableDescriptors;
    }

    public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
        HTableDescriptor[] tableDescriptors = new HTableDescriptor[names.size()];
        for (int i = 0; i < names.size(); i++) {
            tableDescriptors[i] = getTableDescriptor(TableName.valueOf(names.get(i)));
        }
        return tableDescriptors;
    }

    public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
        throw new UnsupportedOperationException("abortProcedure");
    }

    public ProcedureInfo[] listProcedures() throws IOException {
        throw new UnsupportedOperationException("listProcedures");
    }

    public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning) throws IOException {
        throw new UnsupportedOperationException("abortProcedureAsync");
    }

    public void rollWALWriter(ServerName serverName) throws IOException {
        throw new UnsupportedOperationException("rollWALWriter");
    }

    public String[] getMasterCoprocessors() throws IOException {
        throw new UnsupportedOperationException("getMasterCoprocessors");
    }

    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("getCompactionState");
    }

    public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("getCompactionStateForRegion");
    }

    public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
        throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");
    }

    public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("getLastMajorCompactionTimestampForRegion");
    }

    public void snapshot(String snapshotName, TableName tableName) throws IOException, IllegalArgumentException {
        throw new UnsupportedOperationException("snapshot");
    }

    public void snapshot(byte[] snapshotName, TableName tableName) throws IOException, IllegalArgumentException {
        throw new UnsupportedOperationException("snapshot");
    }

    public void snapshot(String snapshotName, TableName tableName, HBaseProtos.SnapshotDescription.Type type) throws IOException, IllegalArgumentException {
        throw new UnsupportedOperationException("snapshot");
    }

    public void snapshot(HBaseProtos.SnapshotDescription snapshot) throws IOException, IllegalArgumentException {
        throw new UnsupportedOperationException("snapshot");
    }

    public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot) throws IOException {
        throw new UnsupportedOperationException("takeSnapshotAsync");
    }

    
    public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshot) throws IOException {
        throw new UnsupportedOperationException("isSnapshotFinished");
    }

    
    public void restoreSnapshot(byte[] snapshotName) throws IOException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    
    public void restoreSnapshot(String snapshotName) throws IOException{
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    
    public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot) throws IOException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    
    public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) throws IOException {
        throw new UnsupportedOperationException("restoreSnapshot");
    }

    
    public void cloneSnapshot(byte[] snapshotName, TableName tableName) throws IOException {
        throw new UnsupportedOperationException("cloneSnapshot");
    }

    
    public void cloneSnapshot(String snapshotName, TableName tableName) throws IOException {
        throw new UnsupportedOperationException("cloneSnapshot");
    }

    
    public void execProcedure(String signature, String instance, Map<String, String> props) throws IOException {
        throw new UnsupportedOperationException("execProcedure");
    }

    
    public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props) throws IOException {
        throw new UnsupportedOperationException("execProcedureWithRet");
    }

    
    public boolean isProcedureFinished(String signature, String instance, Map<String, String> props) throws IOException {
        throw new UnsupportedOperationException("isProcedureFinished");
    }

    
    public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
        throw new UnsupportedOperationException("listSnapshots");
    }

    
    public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
        throw new UnsupportedOperationException("listSnapshots");
    }

    
    public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
        throw new UnsupportedOperationException("listSnapshots");
    }

    
    public void deleteSnapshot(byte[] snapshotName) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshot");
    }

    
    public void deleteSnapshot(String snapshotName) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshot");
    }

    
    public void deleteSnapshots(String regex) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshots");
    }

    
    public void deleteSnapshots(Pattern pattern) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshot");
    }

    
    public void setQuota(QuotaSettings quota) throws IOException {
        throw new UnsupportedOperationException("setQuota");
    }

    
    public QuotaRetriever getQuotaRetriever(QuotaFilter filter) throws IOException {
        throw new UnsupportedOperationException("getQuotaRetriever");
    }

    
    public CoprocessorRpcChannel coprocessorService() {
        throw new UnsupportedOperationException("coprocessorService");
    }

    
    public CoprocessorRpcChannel coprocessorService(ServerName sn) {
        throw new UnsupportedOperationException("coprocessorService");
    }

    
    public void updateConfiguration(ServerName server) throws IOException {
        throw new UnsupportedOperationException("updateConfiguration");
    }
    
    public void updateConfiguration() throws IOException {
        throw new UnsupportedOperationException("updateConfiguration");
    }

    
    public int getMasterInfoPort() throws IOException {
        throw new UnsupportedOperationException("getMasterInfoPort");
    }
    
    public List<SecurityCapability> getSecurityCapabilities() throws IOException {
        throw new UnsupportedOperationException("getSecurityCapabilities");
    }
}

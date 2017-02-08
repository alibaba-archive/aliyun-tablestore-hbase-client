package com.alicloud.tablestore.adaptor.struct;

import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OTableDescriptor {

    private String tableName = null;
    private int maxVersion = 1;
    private int ttl = Integer.MAX_VALUE;
    private long maxTimeDeviation = Integer.MAX_VALUE;
    private List<Pair<byte[], byte[]>> splitKeys = new ArrayList<Pair<byte[], byte[]>>();

    public OTableDescriptor(String tableName) {
        this.tableName = tableName;
    }

    public OTableDescriptor(String tableName, int maxVersion) {
        this.tableName = tableName;
        this.maxVersion = maxVersion;
    }

    public OTableDescriptor(String tableName, int maxVersion, int ttl) {
        this.tableName = tableName;
        this.maxVersion = maxVersion;
        this.ttl = ttl;
    }

    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }

    public int getMaxVersion() {
        return this.maxVersion;
    }

    public void setTimeToLive(int ttl) {
        this.ttl = ttl;
    }

    public int getTimeToLive() {
        return this.ttl;
    }

    public String getTableName() {
        return this.tableName;
    }

    public long getMaxTimeDeviation() {
        return maxTimeDeviation;
    }

    public void setMaxTimeDeviation(long maxTimeDeviation) {
        this.maxTimeDeviation = maxTimeDeviation;
    }

    public void addSplitKey(byte[] begin, byte[] end) {
        splitKeys.add(new Pair<byte[], byte[]>(begin, end));
    }

    public byte[][] getStartKeys() throws IOException {
        byte[][] startKeys = new byte[splitKeys.size()][];
        for (int i = 0; i < splitKeys.size(); i++) {
            startKeys[i] = splitKeys.get(i).getFirst();
        }
        return startKeys;
    }

    public byte[][] getEndKeys() throws IOException {
        byte[][] startKeys = new byte[splitKeys.size()][];
        for (int i = 0; i < splitKeys.size(); i++) {
            startKeys[i] = splitKeys.get(i).getSecond();
        }
        return startKeys;
    }

    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        return new Pair<byte[][], byte[][]>(getStartKeys(), getEndKeys());
    }
}

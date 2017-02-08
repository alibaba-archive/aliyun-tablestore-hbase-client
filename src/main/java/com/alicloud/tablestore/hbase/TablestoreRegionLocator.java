package com.alicloud.tablestore.hbase;

import com.alicloud.tablestore.adaptor.client.OTSAdapter;
import com.alicloud.tablestore.adaptor.struct.OTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TablestoreRegionLocator implements RegionLocator {
    public static long MAX_REGION_AGE_MILLIS = 60 * 1000;

    private final TableName tableName;
    private OTSAdapter tablestoreAdaptor;
    private ServerName serverName;
    private long regionsFetchTimeMillis;
    private List<HRegionLocation> regions;

    public TablestoreRegionLocator(TablestoreConnection connection, TableName tableName) {
        this.tableName = tableName;
        this.tablestoreAdaptor = OTSAdapter.getInstance(connection.getTablestoreConf());
        this.serverName = ServerName.valueOf(connection.getTablestoreConf().getOTSEndpoint(), 0, 0);
        this.regionsFetchTimeMillis = 0;
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
        return getRegionLocation(bytes, false);
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes, boolean reload) throws IOException {
        for(HRegionLocation region : getRegions(reload)) {
            if (region.getRegionInfo().containsRow(bytes)) {
                return region;
            }
        }
        throw new IOException("Region not found for row: " + Bytes.toStringBinary(bytes));
    }

    @Override
    public List<HRegionLocation> getAllRegionLocations() throws IOException {
        return getRegions(false);
    }

    @Override
    public byte[][] getStartKeys() throws IOException {
        return getStartEndKeys().getFirst();
    }

    @Override
    public byte[][] getEndKeys() throws IOException {
        return getStartEndKeys().getSecond();
    }

    @Override
    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        List<HRegionLocation> regions = getAllRegionLocations();
        byte[][] starts = new byte[regions.size()][];
        byte[][] ends = new byte[regions.size()][];
        int i = 0;
        for(HRegionLocation region : regions) {
            starts[i] = region.getRegionInfo().getStartKey();
            ends[i] = region.getRegionInfo().getEndKey();
            i++;
        }
        return Pair.newPair(starts, ends);
    }

    private OTableDescriptor getOTableDescriptor() throws IOException {
        return this.tablestoreAdaptor.describeTable(this.tableName.getNameAsString());
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public void close() throws IOException {
        if (this.tablestoreAdaptor != null) {
            this.tablestoreAdaptor.close();
            this.tablestoreAdaptor = null;
        }
    }

    private synchronized List<HRegionLocation> getRegions(boolean reload) throws IOException {
        if (!reload && regions != null && regionsFetchTimeMillis + MAX_REGION_AGE_MILLIS > System.currentTimeMillis()) {
            return regions;
        }

        List<HRegionLocation> regions = new ArrayList<HRegionLocation>();
        OTableDescriptor oTableDescriptor = getOTableDescriptor();
        for (int i = 0; i < oTableDescriptor.getStartKeys().length; i++) {
            HRegionInfo regionInfo = new HRegionInfo(tableName, oTableDescriptor.getStartKeys()[i], oTableDescriptor.getEndKeys()[i]);
            HRegionLocation region = new HRegionLocation(regionInfo, serverName, i);
            regions.add(region);
        }
        return regions;
    }
}

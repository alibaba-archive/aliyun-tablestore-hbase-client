package com.alicloud.tablestore.adaptor.filter;


public class OColumnPaginationFilter implements OFilter {
    private int limit = 1;
    private int offset = -1;
    private byte[] columnOffset = null;

    public OColumnPaginationFilter(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public OColumnPaginationFilter(int limit, byte[] columnOffset) {
        this.limit = limit;
        this.columnOffset = columnOffset;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getColumnOffset() {
        return columnOffset;
    }
}

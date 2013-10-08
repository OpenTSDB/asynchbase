package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public class FirstKeyOnlyFilter extends ScanFilter {

    private static final String FILTERNAME = "org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter";

    private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

    private boolean foundKV = false;

    public FirstKeyOnlyFilter() {

    }

    @Override
    String getName() {
        return FILTERNAME;
    }

    @Override
    byte[] getNameBytes() {
        return FILTERBYTES;
    }

    @Override
    void serialize(ChannelBuffer buf) {
        //org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
        buf.writeByte((byte)FILTERBYTES.length);         //1
        buf.writeBytes(FILTERBYTES);                    //48
    }

    @Override
    int predictSerializedSize() {
        return 1 + FILTERBYTES.length;
    }
}

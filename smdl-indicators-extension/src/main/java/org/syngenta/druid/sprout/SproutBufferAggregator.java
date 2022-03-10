package org.syngenta.druid.sprout;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SproutBufferAggregator implements BufferAggregator {
    private static final Logger log = new Logger(SproutBufferAggregator.class);
    Long sproutValue;
    SproutBufferAggregator(long value){
        this.sproutValue = value;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putLong(i, 80L);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i) {
        byteBuffer.putLong(i, 90L);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        log.debug("SproutBufferAggregator.get():",sproutValue);
        return this.sproutValue !=null ? this.sproutValue: 20.0;
    }

    @Override
    public float getFloat(ByteBuffer byteBuffer, int i) {
        log.debug("SproutBufferAggregator.getFloat():",sproutValue);
        return this.sproutValue !=null ? (float)this.sproutValue: 20.0f;
    }

    @Override
    public long getLong(ByteBuffer byteBuffer, int i) {
        log.debug("SproutBufferAggregator.getLong():",sproutValue);
        return this.sproutValue !=null ? this.sproutValue: 20l;
    }

    @Override
    public void close() {

    }
}

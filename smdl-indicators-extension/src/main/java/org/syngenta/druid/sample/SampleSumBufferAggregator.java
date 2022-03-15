package org.syngenta.druid.sample;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.SimpleDoubleBufferAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class SampleSumBufferAggregator extends SimpleDoubleBufferAggregator {
    private static final Logger log = new Logger(SampleSumBufferAggregator.class);

    public SampleSumBufferAggregator(BaseDoubleColumnValueSelector selector) {
        super(selector);
    }

    @Override
    public void putFirst(ByteBuffer byteBuffer, int i, double v) {
        log.debug("SampleSumBufferAggregator.putFirst :");
        byteBuffer.putDouble(i, v);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i, double v) {
        log.debug("SampleSumBufferAggregator.aggregate :");
        byteBuffer.putDouble(i, byteBuffer.getDouble(i) + v);
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        log.debug("SampleSumBufferAggregator.init :");
        byteBuffer.putDouble(i, 100.0d);
    }
}

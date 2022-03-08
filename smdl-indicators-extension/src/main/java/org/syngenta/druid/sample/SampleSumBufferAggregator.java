package org.syngenta.druid.sample;

import org.apache.druid.query.aggregation.SimpleDoubleBufferAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class SampleSumBufferAggregator extends SimpleDoubleBufferAggregator {

    public SampleSumBufferAggregator(BaseDoubleColumnValueSelector selector) {
        super(selector);
    }

    @Override
    public void putFirst(ByteBuffer byteBuffer, int i, double v) {
        byteBuffer.putDouble(i, v);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i, double v) {
        byteBuffer.putDouble(i, byteBuffer.getDouble(i) + v);
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putDouble(i, 0.0d);
    }
}

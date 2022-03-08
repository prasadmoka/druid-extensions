package org.syngenta.druid.aggregation;

import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.syngenta.druid.utils.IndicatorUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingulationBufferAggregator implements BufferAggregator {
    private static final Logger log = new Logger(SingulationBufferAggregator.class);
    private final ColumnValueSelector selector;
    private double singulationValue;

    public SingulationBufferAggregator(ColumnValueSelector selector) {
        this.selector = selector;
        this.singulationValue = 0.0;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putDouble(i, 0.0);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i) {
        double value = selector.getDouble();
        System.out.println("SingulationBufferAggregator value is:"+value);
        log.info("SingulationBufferAggregator has this value:",value);
        singulationValue = value;
        byteBuffer.putDouble(i, value);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        return singulationValue;
    }

    @Override
    public float getFloat(ByteBuffer byteBuffer, int i) {
        return (float) singulationValue;
    }

    @Override
    public long getLong(ByteBuffer byteBuffer, int i) {
        return (long) singulationValue;
    }

    @Override
    public void close() {

    }
}

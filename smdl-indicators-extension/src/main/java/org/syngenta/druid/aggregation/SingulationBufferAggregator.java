package org.syngenta.druid.aggregation;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.syngenta.druid.utils.IndicatorUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SingulationBufferAggregator implements BufferAggregator {
    private static final Logger log = new Logger(SingulationBufferAggregator.class);
    private final ColumnValueSelector selector;
    private List<Double> inspectionValues;

    public SingulationBufferAggregator(ColumnValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        log.debug("SingulationBufferAggregator.init():"+i);
        byteBuffer.putDouble(i, 0.0);
        inspectionValues = new ArrayList<>();
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i) {
        double value = selector.getDouble();
        log.debug("SingulationBufferAggregator.aggregate() has this value:"+value);
        inspectionValues.add(value);
        byteBuffer.putDouble(i, value);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        Double singulationValue = 0.0d;
        if(inspectionValues.size() > 1) {
            singulationValue = IndicatorUtils.getSingulation(inspectionValues);
            log.debug("SingulationBufferAggregator.get() has singulation Value is:" + singulationValue);
        }else{//FIXME as this will not work for multiple inspection ids
            singulationValue = byteBuffer.getDouble(i);
        }
        return singulationValue;
    }

    @Override
    public float getFloat(ByteBuffer byteBuffer, int i) {
        return (float) byteBuffer.getDouble(i);
    }

    @Override
    public long getLong(ByteBuffer byteBuffer, int i) {
        return (long) byteBuffer.getDouble(i);
    }

    @Override
    public void close() {

    }
}

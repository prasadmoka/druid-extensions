package org.syngenta.druid.aggregation;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class SingulationAggregator implements Aggregator {
    private static final Logger log = new Logger(SingulationAggregator.class);

    private ColumnValueSelector selector;
    private double singulationValue;

    public SingulationAggregator(ColumnValueSelector selector) {
        this.selector = selector;
        this.singulationValue = 0.0;
    }

    @Override
    public void aggregate() {
        double value = selector.getDouble();
        log.info("SingulationAggregator has this value:",value);
        System.out.println("SingulationAggregator value is:"+value);
        log.info("SingulationAggregator has this value:",value);
        singulationValue = value;
    }

    @Nullable
    @Override
    public Object get() {
        return singulationValue;
    }

    @Override
    public float getFloat() {
        return (float) singulationValue;
    }

    @Override
    public long getLong() {
        return (long) singulationValue;
    }

    @Override
    public void close() {

    }
}

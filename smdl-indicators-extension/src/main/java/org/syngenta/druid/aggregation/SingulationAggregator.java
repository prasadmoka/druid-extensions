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
        System.out.println("SingulationAggregator.aggregate() value is:"+value);
        log.debug("SingulationAggregator.aggregate() has this value:"+value);
        singulationValue = value;
    }

    @Nullable
    @Override
    public Object get() {
        log.debug("SingulationAggregator.get() has this value:"+singulationValue);
        return singulationValue;
    }

    @Override
    public float getFloat() {
        log.debug("SingulationAggregator.getFloat() has this value:"+singulationValue);
        return (float) singulationValue;
    }

    @Override
    public long getLong() {
        log.debug("SingulationAggregator.getLong() has this value:"+singulationValue);
        return (long) singulationValue;
    }

    @Override
    public void close() {

    }
}

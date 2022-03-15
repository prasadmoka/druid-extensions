package org.syngenta.druid.test;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class TestAggregator implements Aggregator {
    private static final Logger log = new Logger(TestAggregator.class);
    private double sum;
    private ColumnValueSelector columnValueSelector;

    public TestAggregator(ColumnValueSelector columnValueSelector) {
        this.sum = 0.0;
        this.columnValueSelector = columnValueSelector;
        log.debug("TestAggregator() has this sum:"+sum);
    }

    @Override
    public void aggregate() {
       sum += columnValueSelector.getDouble();
        log.debug("TestAggregator.aggregate() has this sum:"+sum);
    }

    @Nullable
    @Override
    public Object get()
    {
        log.debug("TestAggregator.get() has this sum:"+sum);
        return sum;
    }

    @Override
    public float getFloat()
    {
        return (float) sum;
    }

    @Override
    public long getLong()
    {
        return (long) sum;
    }

    @Override
    public void close() {

    }
}

package org.syngenta.druid.sample;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

public class SampleSumAggregator implements Aggregator {
    private static final Logger log = new Logger(SampleSumAggregator.class);

    public static final Comparator COMPARATOR = new Ordering() {
        @Override
        public int compare(Object o, Object o1) {
            return Doubles.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
        }
    }.nullsFirst();

    static double combineValues(Object lhs, Object rhs) {
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
    }

    private final BaseDoubleColumnValueSelector selector;

    private double sum;

    public SampleSumAggregator(BaseDoubleColumnValueSelector selector) {
        this.selector = selector;
        this.sum = 0;
    }

    @Override
    public void aggregate() {
        log.debug("SampleSumAggregator.getDouble before:"+selector.getDouble());
        sum += selector.getDouble();
        log.debug("SampleSumAggregator.getDouble:"+sum);
    }

    @Nullable
    @Override
    public Object get() {
        log.debug("SampleSumAggregator.get:"+sum);
        return sum;
    }

    @Override
    public float getFloat() {
        log.debug("SampleSumAggregator.getFloat:"+sum);
        return (float) sum;
    }

    @Override
    public long getLong() {
        log.debug("SampleSumAggregator.getLong:"+sum);
        return (long) sum;
    }

    @Override
    public double getDouble() {
        log.debug("SampleSumAggregator.getDouble:"+sum);
        return sum;
    }

    @Override
    public void close() {

    }


}

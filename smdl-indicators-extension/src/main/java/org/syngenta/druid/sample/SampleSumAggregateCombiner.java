package org.syngenta.druid.sample;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

public class SampleSumAggregateCombiner extends DoubleAggregateCombiner {
    private static final Logger log = new Logger(SampleSumAggregateCombiner.class);

    private double sum;

    @Override
    public void reset(ColumnValueSelector columnValueSelector) {
        sum = columnValueSelector.getDouble();
        log.debug("SampleSumAggregateCombiner.reset:"+sum);
    }

    @Override
    public void fold(ColumnValueSelector columnValueSelector) {
        log.debug("SampleSumAggregateCombiner.fold before:"+sum);
        sum += columnValueSelector.getDouble();
        log.debug("SampleSumAggregateCombiner.fold after:"+sum);
    }

    @Override
    public double getDouble() {
        log.debug("SampleSumAggregateCombiner.getDouble:"+sum);
        return sum;
    }
}

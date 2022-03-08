package org.syngenta.druid.sample;

import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

public class SampleSumAggregateCombiner extends DoubleAggregateCombiner {

    private double sum;

    @Override
    public void reset(ColumnValueSelector columnValueSelector) {
        sum = columnValueSelector.getDouble();
    }

    @Override
    public void fold(ColumnValueSelector columnValueSelector) {
        sum += columnValueSelector.getDouble();
    }

    @Override
    public double getDouble() {
        return sum;
    }
}

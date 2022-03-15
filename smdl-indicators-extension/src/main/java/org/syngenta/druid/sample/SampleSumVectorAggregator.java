package org.syngenta.druid.sample;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SampleSumVectorAggregator implements VectorAggregator {
    private static final Logger log = new Logger(SampleSumVectorAggregator.class);
    private final VectorValueSelector selector;


    public SampleSumVectorAggregator(final VectorValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        log.debug("SampleSumVectorAggregator.init :");
        byteBuffer.putDouble(i, 0);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int position, int i1, int i2) {
        final double[] vector = selector.getDoubleVector();
        log.debug("SampleSumVectorAggregator.aggregate.vector :"+vector);
        double sum = 0;
        for (int i = i1; i < i2; i++) {
            sum += vector[i];
        }
        log.debug("SampleSumVectorAggregator.aggregate.byteBuffer.getDouble(position) :"+byteBuffer.getDouble(position));
        log.debug("SampleSumVectorAggregator.aggregate.sum :"+sum);
        byteBuffer.putDouble(position, byteBuffer.getDouble(position) + sum);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int numRows, int[] positions, @Nullable int[] rows, int positionOffset) {
        final double[] vector = selector.getDoubleVector();
        log.debug("SampleSumVectorAggregator.aggregate1.vector :"+vector);
        log.debug("SampleSumVectorAggregator.aggregate1.rows :"+rows);
        for (int i = 0; i < numRows; i++) {
            final int position = positions[i] + positionOffset;
            byteBuffer.putDouble(position, byteBuffer.getDouble(position) + vector[rows != null ? rows[i] : i]);
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        log.debug("SampleSumVectorAggregator.get: "+byteBuffer.getDouble(i));
        return byteBuffer.getDouble(i);
    }

    @Override
    public void close() {

    }
}

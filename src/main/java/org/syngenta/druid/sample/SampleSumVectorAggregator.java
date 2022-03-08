package org.syngenta.druid.sample;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SampleSumVectorAggregator implements VectorAggregator {
    private final VectorValueSelector selector;


    public SampleSumVectorAggregator(final VectorValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putDouble(i, 0);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int position, int i1, int i2) {
        final double[] vector = selector.getDoubleVector();

        double sum = 0;
        for (int i = i1; i < i2; i++) {
            sum += vector[i];
        }

        byteBuffer.putDouble(position, byteBuffer.getDouble(position) + sum);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int numRows, int[] ints, @Nullable int[] ints1, int i1) {
        final double[] vector = selector.getDoubleVector();

        for (int i = 0; i < numRows; i++) {
            final int position = ints1[i] + i1;
            byteBuffer.putDouble(position, byteBuffer.getDouble(position) + vector[ints1 != null ? ints1[i] : i]);
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        return byteBuffer.getDouble(i);
    }

    @Override
    public void close() {

    }
}

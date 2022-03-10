package org.syngenta.druid.sprout;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SproutVectorAggregator implements VectorAggregator{

    private static final Logger log = new Logger(SproutVectorAggregator.class);

    private final VectorValueSelector selector;

    public SproutVectorAggregator(VectorValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putLong(i, 0L);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int i, int i1, int i2) {
        long[] vector = this.selector.getLongVector();
        log.debug("SproutVectorAggregator.aggregate1:"+vector);
        byteBuffer.putLong(i, vector[0]);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int numRows, int[] positions, @Nullable int[] rows, int positionOffset) {
        long[] vector = this.selector.getLongVector();
        log.debug("SproutVectorAggregator.aggregate2:"+vector);
        byteBuffer.putLong(0, vector[0]);
        log.debug("SproutVectorAggregator.aggregate2, rows are:"+rows);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        log.debug("SproutVectorAggregator.get:");
        return byteBuffer.get(i);
    }

    @Override
    public void close() {

    }
}

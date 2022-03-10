package org.syngenta.druid.aggregation;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.syngenta.druid.utils.IndicatorUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingulationVectorAggregator implements VectorAggregator {
    private static final Logger log = new Logger(SingulationVectorAggregator.class);

    private final VectorValueSelector selector;

    public SingulationVectorAggregator(VectorValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putDouble(i, 0.0);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int position, int startRow, int endRow) {
        double[] vector = this.selector.getDoubleVector();
        log.debug("SingulationVectorAggregator.aggregate1:"+vector);
        byteBuffer.putDouble(position, IndicatorUtils.getSingulation(vector));
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int numRows, int[] positions, @Nullable int[] rows, int positionOffset) {
        double[] vector = this.selector.getDoubleVector();
        log.debug("SingulationVectorAggregator.aggregate2:"+vector);
        for(int i = 0; i < numRows; ++i) {
            int position = positions[i] + positionOffset;
            byteBuffer.putDouble(position, IndicatorUtils.getSingulation(vector));
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int position) {
        log.debug("SingulationVectorAggregator.get:"+position + " and value is:"+byteBuffer.getDouble(position));
        return byteBuffer.getDouble(position);
    }

    @Override
    public void close() {

    }
}

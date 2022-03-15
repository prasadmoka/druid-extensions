package org.syngenta.druid.test;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.syngenta.druid.utils.IndicatorUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestBufferAggregator implements BufferAggregator {
    private static final Logger log = new Logger(TestBufferAggregator.class);
    private ColumnValueSelector selector;


    public TestBufferAggregator(ColumnValueSelector selector) {
        log.debug("TestBufferAggregator() ");
        this.selector = selector;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
        log.debug("TestBufferAggregator.init() ");
        // The amount of space here is given by getMaxIntermediateSize in the factory, 8 Bytes for double
        buf.putDouble(position, 0.0d);

    }

    @Override
    public final void aggregate(ByteBuffer buf, int position)
    {
        log.debug("TestBufferAggregator.aggregate(): selector value:"+selector.getDouble() +" at position:"+position+" with bufValue:"+buf.getDouble(position));
        buf.putDouble(position, buf.getDouble(position) + selector.getDouble());

    }

    @Nullable
    @Override
    public final Object get(ByteBuffer buf, int position)
    {
        log.debug("TestBufferAggregator.get() has value:"+buf.getDouble(position)+" at position:"+position);
        return buf.getDouble(position);
    }

    @Override
    public final float getFloat(ByteBuffer buf, int position)
    {
        return (float) buf.getDouble(position);
    }

    @Override
    public final long getLong(ByteBuffer buf, int position)
    {
        return (long) buf.getDouble(position);
    }


    @Override
    public void close() {

    }
}

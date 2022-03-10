package org.syngenta.druid.sprout;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;

import javax.annotation.Nullable;

public class SproutAggregator implements Aggregator {

    private static final Logger log = new Logger(SproutAggregator.class);

    Long sproutValue;
    SproutAggregator(long value){
        this.sproutValue = value;
    }

    @Override
    public void aggregate() {
        log.debug("SproutAggregator.aggregate() has this value:"+this.sproutValue);
    }

    @Nullable
    @Override
    public Object get() {
        log.debug("SproutAggregator.get() has this value:"+this.sproutValue);
        return this.sproutValue !=null ? this.sproutValue: 10.0;
    }

    @Override
    public float getFloat() {
        log.debug("SproutAggregator.getFloat() has this value:"+this.sproutValue);
        return this.sproutValue !=null ? (float)this.sproutValue: 10.0f;
    }

    @Override
    public long getLong() {
        log.debug("SproutAggregator.getDouble() has this value:"+this.sproutValue);
        return this.sproutValue !=null ? this.sproutValue: 10l;
    }

    @Override
    public void close() {

    }
}

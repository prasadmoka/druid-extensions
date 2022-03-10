package org.syngenta.druid.sample;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class SampleSumAggregatorFactory extends SimpleDoubleAggregatorFactory {
    private static final Logger log = new Logger(SampleSumAggregatorFactory.class);

    private final Supplier<byte[]> cacheKey;

    @JsonCreator
    public SampleSumAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("expression") @Nullable String expression,
            @JacksonInject ExprMacroTable macroTable
    ) {
        super(macroTable, name, fieldName, expression);
        this.cacheKey = AggregatorUtil.getSimpleAggregatorCacheKeySupplier(
                AggregatorUtil.DOUBLE_SUM_CACHE_TYPE_ID,
                fieldName,
                fieldExpression
        );
        log.debug("SampleSumAggregatorFactory() with :"+name +" and "+fieldName);
    }

    public SampleSumAggregatorFactory(String name, String fieldName) {
        this(name, fieldName, null, ExprMacroTable.nil());
    }

    @Override
    protected double nullValue() {
        log.debug("SampleSumAggregatorFactory.nullValue :");
        return 0.0d;
    }

    @Override
    protected Aggregator buildAggregator(BaseDoubleColumnValueSelector baseDoubleColumnValueSelector) {
        log.debug("SampleSumAggregatorFactory.buildAggregator :");
        return new SampleSumAggregator(baseDoubleColumnValueSelector);
    }

    @Override
    protected BufferAggregator buildBufferAggregator(BaseDoubleColumnValueSelector baseDoubleColumnValueSelector) {
        log.debug("SampleSumAggregatorFactory.buildBufferAggregator :");
        return new SampleSumBufferAggregator(baseDoubleColumnValueSelector);
    }

    @Override
    protected VectorAggregator factorizeVector(
            VectorColumnSelectorFactory columnSelectorFactory,
            VectorValueSelector selector
    ) {
        log.debug("SampleSumAggregatorFactory.factorizeVector :");
        return new SampleSumVectorAggregator(selector);
    }

    @Override
    @Nullable
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        log.debug("SampleSumAggregatorFactory.combine :");
        if (rhs == null) {
            return lhs;
        }
        if (lhs == null) {
            return rhs;
        }
        return SampleSumAggregator.combineValues(lhs, rhs);
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        log.debug("SampleSumAggregatorFactory.makeAggregateCombiner:");
        return new SampleSumAggregateCombiner();
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        log.debug("SampleSumAggregatorFactory.getCombiningFactory:");
        return new SampleSumAggregatorFactory(name, fieldName, null, macroTable);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new SampleSumAggregatorFactory(name, fieldName, expression, macroTable));
    }

    @Override
    public byte[] getCacheKey() {
        return cacheKey.get();
    }

    @Override
    public String toString() {
        return "SampleSumAggregatorFactory{" +
                "fieldName='" + fieldName + '\'' +
                ", expression='" + expression + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

}

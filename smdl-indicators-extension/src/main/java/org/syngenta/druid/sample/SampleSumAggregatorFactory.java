package org.syngenta.druid.sample;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class SampleSumAggregatorFactory extends SimpleDoubleAggregatorFactory {

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
    }

    public SampleSumAggregatorFactory(String name, String fieldName) {
        this(name, fieldName, null, ExprMacroTable.nil());
    }

    @Override
    protected double nullValue() {
        return 0.0d;
    }

    @Override
    protected Aggregator buildAggregator(BaseDoubleColumnValueSelector baseDoubleColumnValueSelector) {
        return new SampleSumAggregator(baseDoubleColumnValueSelector);
    }

    @Override
    protected BufferAggregator buildBufferAggregator(BaseDoubleColumnValueSelector baseDoubleColumnValueSelector) {
        return new SampleSumBufferAggregator(baseDoubleColumnValueSelector);
    }

    @Override
    protected VectorAggregator factorizeVector(
            VectorColumnSelectorFactory columnSelectorFactory,
            VectorValueSelector selector
    ) {
        return new SampleSumVectorAggregator(selector);
    }

    @Override
    @Nullable
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
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
        return new SampleSumAggregateCombiner();
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new SampleSumAggregatorFactory(name, name, null, macroTable);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new SampleSumAggregatorFactory(fieldName, fieldName, expression, macroTable));
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

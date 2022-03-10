package org.syngenta.druid.sprout;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SproutAggregatorFactory extends AggregatorFactory {
    private static final Logger log = new Logger(SproutAggregatorFactory.class);
    private static final Comparator<Long> VALUE_COMPARATOR = Long::compare;
    private final String name;
    private final String fieldName;

    @JsonCreator
    public SproutAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") final String fieldName) {
        log.debug("SproutAggregator() with :"+name +" and "+ fieldName);
        this.name = name;
        this.fieldName = fieldName;
    }


    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
       Long value = columnSelectorFactory.makeColumnValueSelector(fieldName).getLong();
        log.debug("SproutAggregator.factorize():"+value);
        return new SproutAggregator(value);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        Long value = columnSelectorFactory.makeColumnValueSelector(fieldName).getLong();
        log.debug("SproutAggregator.factorizeBuffered():"+value);
        return new SproutBufferAggregator(value);
    }

    @Override
    public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory) {
        log.debug("SproutAggregator.factorizeVector()");
        return new SproutVectorAggregator(selectorFactory.makeValueSelector(fieldName));
    }

    @Override
    public boolean canVectorize(ColumnInspector columnInspector) {
        return true;
    }

    @Override
    public Comparator getComparator() {
        return VALUE_COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        long combinedVal = ((Number) lhs).longValue() + ((Number) rhs).longValue();
        log.debug("SproutAggregator.combine() has this combinedVal:"+combinedVal);
        return combinedVal;
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new SproutAggregatorFactory(this.name, this.fieldName);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new SproutAggregatorFactory(this.name, this.fieldName));
    }

    @Override
    public Object deserialize(Object object) {
        log.debug("SproutAggregatorFactory.deserialize object is:"+ object);
        Object o = object instanceof String ? Double.parseDouble((String) object) : object;
        return o;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object o) {
        log.debug("SproutAggregatorFactory.finalizeComputation value is:"+ o);
        return o;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName(){
        return fieldName;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(this.fieldName);
    }

    @Override
    public ValueType getType() {
        return ValueType.LONG;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.LONG;
    }

    @Override
    public int getMaxIntermediateSize() {
        return 10;
    }

    @Override
    public byte[] getCacheKey() {
        return new byte[0];
    }
}

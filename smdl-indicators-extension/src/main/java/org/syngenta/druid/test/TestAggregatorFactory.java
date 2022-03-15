package org.syngenta.druid.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestAggregatorFactory extends AggregatorFactory {
    private static final byte[] CACHE_KEY_PREFIX = new byte[]{(byte) 0xFF, (byte) 0x00};
    private static final Logger log = new Logger(TestAggregatorFactory.class);
    private static final Comparator<Double> VALUE_COMPARATOR = Double::compare;
    private final String name;
    private final String fieldName;

    @JsonCreator
    public TestAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") final String fieldName) {
        log.debug("TestAggregatorFactory() with :"+name +" and "+ fieldName);
        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
        return new TestAggregator(columnSelectorFactory.makeColumnValueSelector(this.fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        return new TestBufferAggregator(columnSelectorFactory.makeColumnValueSelector(this.fieldName));
    }

    @Override
    public Comparator getComparator() {
        return VALUE_COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        log.debug("TestAggregatorFactory.combine() ");
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new TestAggregatorFactory(this.name, this.name);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new TestAggregatorFactory(this.fieldName, this.fieldName));
    }

    @Override
    public Object deserialize(Object object) {
        log.debug("TestAggregatorFactory.deserialize() ");
        // handle "NaN" / "Infinity" values serialized as strings in JSON
        if (object instanceof String) {
            return Double.parseDouble((String) object);
        }
        return object;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object o) {
        log.debug("TestAggregatorFactory.finalizeComputation() "+o);
        return o;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(this.fieldName);
    }

    @Override
    public ValueType getType() {
        return ValueType.DOUBLE;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.DOUBLE;
    }

    @Override
    public int getMaxIntermediateSize() {
        return Double.BYTES;
    }

    @Override
    public byte[] getCacheKey() {
        log.debug("TestAggregatorFactory.getCacheKey() ");
        byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(this.fieldName);
        return ByteBuffer.allocate(2 + fieldNameBytes.length)
                .put(CACHE_KEY_PREFIX)
                .put(fieldNameBytes)
                .array();
    }
}

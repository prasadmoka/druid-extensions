package org.syngenta.druid.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
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

public class SingulationAggregatorFactory extends AggregatorFactory {

    private static final Logger log = new Logger(SingulationAggregatorFactory.class);
    private final String name;
    private final String fieldName;

    @JsonCreator
    public SingulationAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName
    )
    {
        log.info("SingulationAggregatorFactory()");
        this.name = Preconditions.checkNotNull(name, "name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    }


    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
        log.info("SingulationAggregatorFactory()");
        return new SingulationAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        log.info("SingulationAggregatorFactory()");
        return new SingulationBufferAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory) {
        log.info("SingulationAggregatorFactory()");
        return new SingulationVectorAggregator(selectorFactory.makeValueSelector(fieldName));
    }

    @Override
    public boolean canVectorize(ColumnInspector columnInspector) {
        return true;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        return combineValues(lhs,rhs);
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new SingulationAggregatorFactory(name,fieldName);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new SingulationAggregatorFactory(this.fieldName, this.fieldName));
    }

    @Override
    public Object deserialize(Object object) {
        return object instanceof String ? Double.parseDouble((String)object) : object;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object obj) {
        return obj;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(this.fieldName);
    }

    @Override
    public ValueType getType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public int getMaxIntermediateSize() {
        return 0;
    }

    @Override
    public byte[] getCacheKey() {
        return new byte[0];
    }

    public static final Comparator COMPARATOR = (new Ordering() {
        public int compare(Object o, Object o1) {
            return Doubles.compare(((Number)o).doubleValue(), ((Number)o1).doubleValue());
        }
    }).nullsFirst();

    static double combineValues(Object lhs, Object rhs) {
        return ((Number)lhs).doubleValue() + ((Number)rhs).doubleValue();
    }
}

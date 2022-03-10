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
        log.debug("SingulationAggregatorFactory() "+ name +" and " +fieldName);
        this.name = Preconditions.checkNotNull(name, "name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    }


    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
        Double value = columnSelectorFactory.makeColumnValueSelector(fieldName).getDouble();
        log.debug("SingulationAggregatorFactory.factorize():"+value);
        return new SingulationAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        Double value = columnSelectorFactory.makeColumnValueSelector(fieldName).getDouble();
        log.debug("SingulationAggregatorFactory.factorizeBuffered():"+value);
        return new SingulationBufferAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory) {
        double[] value = selectorFactory.makeValueSelector(fieldName).getDoubleVector();
        log.debug("SingulationAggregatorFactory.factorizeVector():"+value);
        return new SingulationVectorAggregator(selectorFactory.makeValueSelector(fieldName));
    }

    @Override
    public boolean canVectorize(ColumnInspector columnInspector) {
        log.debug("SingulationAggregatorFactory.canVectorize");
        return true;
    }

    @Override
    public Comparator getComparator() {
        log.debug("SingulationAggregatorFactory.getComparator");
        return COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        log.debug("SingulationAggregatorFactory.combine");
        return combineValues(lhs,rhs);
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        log.debug("SingulationAggregatorFactory.getCombiningFactory");
        return new SingulationAggregatorFactory(name,fieldName);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        log.debug("SingulationAggregatorFactory.getRequiredColumns");
        return Collections.singletonList(new SingulationAggregatorFactory(this.fieldName, this.fieldName));
    }

    @Override
    public Object deserialize(Object object) {
        Object o = object instanceof String ? Double.parseDouble((String) object) : object;
        log.debug("SingulationAggregatorFactory.deserialize object is:"+ o);
        return o;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object obj) {
        log.debug("SingulationAggregatorFactory.finalizeComputation:"+obj);
        return obj;
    }

    @Override
    @JsonProperty
    public String getName() {
        //log.debug("SingulationAggregatorFactory.getName");
        return name;
    }

    @JsonProperty
    public String getFieldName() {
       // log.debug("SingulationAggregatorFactory.getFieldName");
        return fieldName;
    }

    @Override
    public List<String> requiredFields() {
        log.debug("SingulationAggregatorFactory.requiredFields");
        return Collections.singletonList(this.fieldName);
    }

    @Override
    public ValueType getType() {
        log.debug("SingulationAggregatorFactory.getType");
        return ValueType.DOUBLE;
    }

    @Override
    public ValueType getFinalizedType() {
        log.debug("SingulationAggregatorFactory.getFinalizedType");
        return ValueType.DOUBLE;
    }

    @Override
    public int getMaxIntermediateSize() {
        log.debug("SingulationAggregatorFactory.getMaxIntermediateSize");
        return 8;
    }

    @Override
    public byte[] getCacheKey() {
        log.debug("SingulationAggregatorFactory.getCacheKey");
        return new byte[0];
    }

    public static final Comparator COMPARATOR = (new Ordering() {
        public int compare(Object o, Object o1) {
            return Doubles.compare(((Number)o).doubleValue(), ((Number)o1).doubleValue());
        }
    }).nullsFirst();

    static double combineValues(Object lhs, Object rhs) {
        log.debug("SingulationAggregatorFactory.combineValues");
        return ((Number)lhs).doubleValue() + ((Number)rhs).doubleValue();
    }
}

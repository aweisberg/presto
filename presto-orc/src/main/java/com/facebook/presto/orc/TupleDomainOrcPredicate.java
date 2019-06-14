/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.RangeStatistics;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.EquatableValueSet.ValueEntry;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.hive.common.util.BloomFilter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TupleDomainOrcPredicate<C>
        implements OrcPredicate
{
    private final TupleDomain<C> effectivePredicate;
    private final TupleDomain<C> compactEffectivePredicate;
    private final List<ColumnReference<C>> columnReferences;

    private final boolean orcBloomFiltersEnabled;

    public TupleDomainOrcPredicate(TupleDomain<C> effectivePredicate, List<ColumnReference<C>> columnReferences, boolean orcBloomFiltersEnabled)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        // TODO Use hiveClientConfig.getDomainCompactionThreshold()
        this.compactEffectivePredicate = toCompactTupleDomain(effectivePredicate, 100);
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
        this.orcBloomFiltersEnabled = orcBloomFiltersEnabled;
    }

    private static <C> TupleDomain<C> toCompactTupleDomain(TupleDomain<C> effectivePredicate, int threshold)
    {
        ImmutableMap.Builder<C, Domain> builder = ImmutableMap.builder();
        effectivePredicate.getDomains().ifPresent(domains -> {
            for (Map.Entry<C, Domain> entry : domains.entrySet()) {
                C hiveColumnHandle = entry.getKey();

                ValueSet values = entry.getValue().getValues();
                ValueSet compactValueSet = values.getValuesProcessor().<Optional<ValueSet>>transform(
                        ranges -> ranges.getRangeCount() > threshold ? Optional.of(ValueSet.ofRanges(ranges.getSpan())) : Optional.empty(),
                        discreteValues -> discreteValues.getValues().size() > threshold ? Optional.of(ValueSet.all(values.getType())) : Optional.empty(),
                        allOrNone -> Optional.empty())
                        .orElse(values);
                builder.put(hiveColumnHandle, Domain.create(compactValueSet, entry.getValue().isNullAllowed()));
            }
        });
        return TupleDomain.withColumnDomains(builder.build());
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
    {
        Optional<Map<C, Domain>> optionalEffectivePredicateDomains = compactEffectivePredicate.getDomains();
        if (!optionalEffectivePredicateDomains.isPresent()) {
            // effective predicate is none, so skip this section
            return false;
        }
        Map<C, Domain> effectivePredicateDomains = optionalEffectivePredicateDomains.get();

        for (ColumnReference<C> columnReference : columnReferences) {
            Domain predicateDomain = effectivePredicateDomains.get(columnReference.getColumn());
            if (predicateDomain == null) {
                // no predicate on this column, so we can't exclude this section
                continue;
            }
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnReference.getOrdinal());
            if (columnStatistics == null) {
                // no statistics for this column, so we can't exclude this section
                continue;
            }

            if (!columnOverlaps(columnReference, predicateDomain, numberOfRows, columnStatistics)) {
                return false;
            }
        }

        // this section was not excluded
        return true;
    }

    private boolean columnOverlaps(ColumnReference<C> columnReference, Domain predicateDomain, long numberOfRows, ColumnStatistics columnStatistics)
    {
        if (columnReference.getColumn() instanceof ColumnHandle && ((ColumnHandle) columnReference.getColumn()).getSubfieldPath() != null) {
            return true;
        }
        Domain stripeDomain = getDomain(columnReference.getType(), numberOfRows, columnStatistics);
        if (!stripeDomain.overlaps(predicateDomain)) {
            // there is no overlap between the predicate and this column
            return false;
        }

        // if bloom filters are not enabled, we can not restrict the range overlap
        if (!orcBloomFiltersEnabled) {
            return true;
        }

        // if there an overlap in null values, the bloom filter can not eliminate the overlap
        if (predicateDomain.isNullAllowed() && stripeDomain.isNullAllowed()) {
            return true;
        }

        // extract the discrete values from the predicate
        Optional<Collection<Object>> discreteValues = extractDiscreteValues(predicateDomain.getValues());
        if (!discreteValues.isPresent()) {
            // values are not discrete, so we can't exclude this section
            return true;
        }

        HiveBloomFilter bloomFilter = columnStatistics.getBloomFilter();
        if (bloomFilter == null) {
            // no bloom filter so we can't exclude this section
            return true;
        }

        // if none of the discrete predicate values are found in the bloom filter, there is no overlap and the section should be skipped
        if (discreteValues.get().stream().noneMatch(value -> checkInBloomFilter(bloomFilter, value, stripeDomain.getType()))) {
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public static Optional<Collection<Object>> extractDiscreteValues(ValueSet valueSet)
    {
        return valueSet.getValuesProcessor().transform(
                ranges -> {
                    ImmutableList.Builder<Object> discreteValues = ImmutableList.builder();
                    for (Range range : ranges.getOrderedRanges()) {
                        if (!range.isSingleValue()) {
                            return Optional.empty();
                        }
                        discreteValues.add(range.getSingleValue());
                    }
                    return Optional.of(discreteValues.build());
                },
                discreteValues -> Optional.of(discreteValues.getValues()),
                allOrNone -> allOrNone.isAll() ? Optional.empty() : Optional.of(ImmutableList.of()));
    }

    // checks whether a value part of the effective predicate is likely to be part of this bloom filter
    @VisibleForTesting
    public static boolean checkInBloomFilter(BloomFilter bloomFilter, Object predicateValue, Type sqlType)
    {
        if (sqlType == TINYINT || sqlType == SMALLINT || sqlType == INTEGER || sqlType == BIGINT) {
            return bloomFilter.testLong(((Number) predicateValue).longValue());
        }

        if (sqlType == DOUBLE) {
            return bloomFilter.testDouble((Double) predicateValue);
        }

        if (sqlType instanceof VarcharType || sqlType instanceof VarbinaryType) {
            return bloomFilter.test(((Slice) predicateValue).getBytes());
        }

        // todo support DECIMAL, FLOAT, DATE, TIMESTAMP, and CHAR
        return true;
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
    {
        if (rowCount == 0) {
            return Domain.none(type);
        }

        if (columnStatistics == null) {
            return Domain.all(type);
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = columnStatistics.getNumberOfValues() != rowCount;

        if (type.getJavaType() == boolean.class && columnStatistics.getBooleanStatistics() != null) {
            BooleanStatistics booleanStatistics = columnStatistics.getBooleanStatistics();

            boolean hasTrueValues = (booleanStatistics.getTrueValueCount() != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != booleanStatistics.getTrueValueCount());
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(BOOLEAN);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(BOOLEAN, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(BOOLEAN, false), hasNullValue);
            }
        }
        else if (isShortDecimal(type) && columnStatistics.getDecimalStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> rescale(value, (DecimalType) type).unscaledValue().longValue());
        }
        else if (isLongDecimal(type) && columnStatistics.getDecimalStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> encodeUnscaledValue(rescale(value, (DecimalType) type).unscaledValue()));
        }
        else if (isCharType(type) && columnStatistics.getStringStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getStringStatistics(), value -> truncateToLengthAndTrimSpaces(value, type));
        }
        else if (isVarcharType(type) && columnStatistics.getStringStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getStringStatistics());
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.DATE) && columnStatistics.getDateStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDateStatistics(), value -> (long) value);
        }
        else if (type.getJavaType() == long.class && columnStatistics.getIntegerStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getIntegerStatistics());
        }
        else if (type.getJavaType() == double.class && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDoubleStatistics());
        }
        else if (REAL.equals(type) && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDoubleStatistics(), value -> (long) floatToRawIntBits(value.floatValue()));
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<T> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = requireNonNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("column", column)
                    .add("ordinal", ordinal)
                    .add("type", type)
                    .toString();
        }
    }

    @Override
    public Map<Integer, Filter> getFilters()
    {
        Optional<Map<C, Domain>> optionalEffectivePredicateDomains = effectivePredicate.getDomains();
        if (!optionalEffectivePredicateDomains.isPresent()) {
            // No filters
            return ImmutableMap.of();
        }

        Map<C, Domain> effectivePredicateDomains = optionalEffectivePredicateDomains.get();

        Map<Integer, Filter> filters = new HashMap();
        for (Map.Entry<C, Domain> entry : effectivePredicateDomains.entrySet()) {
            Domain predicateDomain = entry.getValue();
            C column = entry.getKey();
            checkState(column instanceof ColumnHandle, "Unexpected column handle type: " + column.getClass().getSimpleName());
            ColumnHandle columnHandle = (ColumnHandle) column;
            SubfieldPath subfield = columnHandle.getSubfieldPath();
            ColumnHandle topLevelColumn = subfield == null ? columnHandle : columnHandle.createSubfieldColumnHandle(null);
            ColumnReference<C> columnReference = null;
            for (ColumnReference<C> c : columnReferences) {
                if (c.getColumn().equals(topLevelColumn) || c.getColumn().equals(columnHandle)) {
                    columnReference = c;
                    break;
                }
            }
            // TODO Figure out how to remove this check.
            // This can happen if a column is a partition column or a synthetic $bucket column
            // Is there a way to exclude predicates on these columns at the creation time.
            if (columnReference == null) {
                continue;
            }
            Filter filter;
            ValueSet values = predicateDomain.getValues();
            boolean nullAllowed = predicateDomain.isNullAllowed();
            if (values instanceof EquatableValueSet) {
                Set<ValueEntry> entries = ((EquatableValueSet) values).getEntries();
                if (entries.isEmpty()) {
                    filter = nullAllowed ? Filters.isNull() : Filters.isNotNull();
                }
                else {
                    throw new UnsupportedOperationException("EquatableValueSet with non empty entries is not supported");
                }
            }
            else if (values instanceof SortedRangeSet) {
                List<Range> ranges = ((SortedRangeSet) values).getOrderedRanges();
                Type type = predicateDomain.getType();
                if (ranges.isEmpty() && nullAllowed) {
                    filter = Filters.isNull();
                }
                else if (ranges.size() == 1) {
                    filter = createRangeFilter(type, ranges.get(0), nullAllowed);
                }
                else if (isBooleanNotEquals(type, ranges)) {
                    filter = booleanNotEqualsToFilter(ranges, nullAllowed);
                }
                else {
                    List<Filter> rangeFilters = ranges.stream()
                            .map(r -> createRangeFilter(type, r, false))
                            .filter(f -> f != Filters.alwaysFalse())
                            .collect(toImmutableList());
                    if (rangeFilters.isEmpty()) {
                        filter = nullAllowed ? Filters.isNull() : Filters.alwaysFalse();
                    }
                    else {
                        filter = Filters.createMultiRange(rangeFilters, nullAllowed);
                    }
                }
            }
            else {
                throw new UnsupportedOperationException("Unexpected TupleDomain: " + values.getClass().getSimpleName());
            }
            addFilter(columnReference.getOrdinal(), subfield, filter, filters);
        }
        return filters;
    }

    private static Filter createRangeFilter(Type type, Range range, boolean nullAllowed)
    {
        if (range.isAll()) {
            return nullAllowed ? null : Filters.isNotNull();
        }
        if (isVarcharType(type) || type instanceof CharType) {
            return varcharRangeToFilter(range, nullAllowed);
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP) {
            return bigintRangeToFilter(range, nullAllowed);
        }
        if (type == DOUBLE) {
            return doubleRangeToFilter(range, nullAllowed);
        }
        if (type == REAL) {
            return floatRangeToFilter(range, nullAllowed);
        }
        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                return bigintRangeToFilter(range, nullAllowed);
            }
            return longDecimalRangeToFilter(range, nullAllowed);
        }
        if (type == BOOLEAN) {
            checkArgument(range.isSingleValue());
            boolean booleanValue = ((Boolean) range.getSingleValue()).booleanValue();
            return  new Filters.BooleanValue(booleanValue, nullAllowed);
        }

        throw new UnsupportedOperationException("Unsupported type: " + type.getDisplayName());
    }

    private static void addFilter(Integer ordinal, SubfieldPath subfield, Filter filter, Map<Integer, Filter> filters)
    {
        if (subfield == null) {
            filters.put(ordinal, filter);
            return;
        }

        Filter topFilter = filters.get(ordinal);
        Filter newFilter = combineFilter(topFilter, filter);
        if (newFilter != topFilter) {
            filters.put(ordinal, newFilter);
            if (!(newFilter instanceof Filters.StructFilter)) {
                return;
            }
            topFilter = newFilter;
        }
        verify(topFilter instanceof Filters.StructFilter);
        Filters.StructFilter structFilter = (Filters.StructFilter) topFilter;
        int depth = subfield.getPathElements().size();
        for (int i = 1; i < depth; i++) {
            Filter memberFilter = structFilter.getMember(subfield.getPathElements().get(i));
            if (i == depth - 1) {
                if (memberFilter == Filters.alwaysFalse()) {
                    return;
                }
                if (memberFilter != null && memberFilter instanceof Filters.StructFilter) {
                    if (filter == Filters.isNotNull()) {
                        return;
                    }
                    if (filter == Filters.isNull()) {
                        if (!((Filters.StructFilter) memberFilter).isOnlyIsNulls()) {
                            filter = Filters.alwaysFalse();
                        }
                    }
                }
                structFilter.addMember(subfield.getPathElements().get(i), filter);
                return;
            }
            if (memberFilter == null) {
                memberFilter = new Filters.StructFilter();
                structFilter.addMember(subfield.getPathElements().get(i), memberFilter);
                structFilter = (Filters.StructFilter) memberFilter;
            }
            else {
                newFilter = combineFilter(memberFilter, filter);
                if (newFilter != memberFilter) {
                    structFilter.addMember(subfield.getPathElements().get(i), newFilter);
                    if (!(newFilter instanceof Filters.StructFilter)) {
                        return;
                    }
                    memberFilter = newFilter;
                }
                verify(memberFilter instanceof Filters.StructFilter);
                structFilter = (Filters.StructFilter) memberFilter;
            }
        }
    }

    // A struct can have IsNull, IsNotNull or a StructFilter.
    // AlwaysFalse + anything is AlwaysFalse
    // IsNotNull + anything = StructFilter.
    // IsNull + IsNull is unchanged. Isnull otherwise is AlwaysFalse.
    // StructFilter + IsNull is always false if StructFilter contains anything except is null
    private static Filter combineFilter(Filter structFilter, Filter memberFilter)
    {
        if (structFilter == null) {
            return new Filters.StructFilter();
        }
        if (structFilter == Filters.alwaysFalse()) {
            return structFilter;
        }
        if (structFilter == Filters.isNull()) {
            if (memberFilter == Filters.isNull()) {
                return structFilter;
            }
            return Filters.alwaysFalse();
        }
        if (structFilter == Filters.isNotNull()) {
            return new Filters.StructFilter();
        }
        return structFilter;
    }

    private static Filter bigintRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        long lowerLong = low.isLowerUnbounded() ? Long.MIN_VALUE : (long) low.getValue();
        long upperLong = high.isUpperUnbounded() ? Long.MAX_VALUE : (long) high.getValue();
        if (!high.isUpperUnbounded() && high.getBound() == Marker.Bound.BELOW) {
            --upperLong;
        }
        if (!low.isLowerUnbounded() && low.getBound() == Marker.Bound.ABOVE) {
            ++lowerLong;
        }
        if (upperLong < lowerLong) {
            return Filters.alwaysFalse();
        }
        return new Filters.BigintRange(lowerLong, upperLong, nullAllowed);
    }

    private static Filter doubleRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        double lowerDouble = low.isLowerUnbounded() ? Double.MIN_VALUE : (double) low.getValue();
        double upperDouble = high.isUpperUnbounded() ? Double.MAX_VALUE : (double) high.getValue();
        return new Filters.DoubleRange(
                lowerDouble,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperDouble,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter floatRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        float lowerFloat = low.isLowerUnbounded() ? Float.MIN_VALUE : intBitsToFloat(toIntExact((long) low.getValue()));
        float upperFloat = high.isUpperUnbounded() ? Float.MAX_VALUE : intBitsToFloat(toIntExact((long) high.getValue()));
        return new Filters.FloatRange(
                lowerFloat,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperFloat,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter longDecimalRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        return new Filters.LongDecimalRange(
                low.isLowerUnbounded() ? 0 : ((Slice) low.getValue()).getLong(0),
                low.isLowerUnbounded() ? 0 : ((Slice) low.getValue()).getLong(SIZE_OF_LONG),
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                high.isLowerUnbounded() ? 0 : ((Slice) high.getValue()).getLong(0),
                high.isLowerUnbounded() ? 0 : ((Slice) high.getValue()).getLong(SIZE_OF_LONG),
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter varcharRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Marker.Bound lowerBound = low.getBound();
        Marker.Bound upperBound = high.getBound();
        Slice lowerValue = low.isLowerUnbounded() ? null : (Slice) low.getValue();
        Slice upperValue = high.isUpperUnbounded() ? null : (Slice) high.getValue();
        return new Filters.BytesRange(lowerValue == null ? null : lowerValue.getBytes(),
                lowerBound == Marker.Bound.EXACTLY,
                upperValue == null ? null : upperValue.getBytes(),
                                      upperBound == Marker.Bound.EXACTLY, nullAllowed);
    }

    private static boolean isBooleanNotEquals(Type type, List<Range> ranges)
    {
        return type == BOOLEAN && ranges.size() == 2 && !ranges.get(0).isSingleValue();
    }

    private static Filter booleanNotEqualsToFilter(List<Range> ranges, boolean nullAllowed)
    {
        // A boolean with more than one value means <> true or <> false.
        checkArgument(ranges.size() == 2);
        Object value;
        Range range1 = ranges.get(0);
        Range range2 = ranges.get(1);
        Marker low1 = range1.getLow();
        Marker high1 = range1.getHigh();
        Marker low2 = range2.getLow();
        Marker high2 = range2.getHigh();
        if (low1.getValueBlock().isPresent()) {
            value = (Boolean)low1.getValue();
            checkState(value.equals(high2.getValue()));
        }
        else {
            value = high1.getValue();
            checkState(value.equals(low2.getValue()));
        }
        return new Filters.BooleanValue(!((Boolean)value).booleanValue(), nullAllowed);
    }
}

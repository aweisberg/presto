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
package com.facebook.presto.orc.reader;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.newIntArrayForReuse;
import static com.facebook.presto.orc.ResizedArrays.resize;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

abstract class AbstractLongStreamReader
            extends NullWrappingColumnReader
{
    protected long[] values;
    protected int[] intValues;
    protected short[] shortValues;

    AbstractLongStreamReader()
    {
        super(OptionalInt.of(SIZE_OF_LONG));
    }

    @Override
    public void erase(int end)
    {
        if (values == null) {
            return;
        }
        checkEnoughValues(end);
        numValues -= end;
        if (numValues > 0) {
            System.arraycopy(values, end, values, 0, numValues);
            if (valueIsNull != null) {
                System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
            }
        }
    }

    @Override
    public void compactValues(int[] positions, int base, int numPositions)
    {
        if (outputChannelSet) {
            StreamReaders.compactArrays(positions, base, numPositions, values, valueIsNull);
            numValues = base + numPositions;
        }
        compactQualifyingSet(positions, numPositions);
    }

    @Override
    protected void shiftUp(int from, int to)
    {
        values[to] = values[from];
    }

    @Override
    protected void writeNull(int i)
    {
        // No action. values[i] is undefined if valueIsNull[i] == true.
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        checkEnoughValues(numFirstRows);
        if (mayReuse) {
            if (type == INTEGER || type == DATE) {
                if (intValues == null || intValues.length < numValues) {
                    intValues = newIntArrayForReuse(numValues);
                }
                for (int i = 0; i < numFirstRows; i++) {
                    intValues[i] = (int) values[i];
                }
                return new IntArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), intValues);
            }
            else if (type == BIGINT) {
                return new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
            }
            else if (type == SMALLINT) {
                if (shortValues == null || shortValues.length < numValues) {
                    shortValues = new short[numValues];
                }
                for (int i = 0; i < numFirstRows; i++) {
                    shortValues[i] = (short) values[i];
                }
                return new ShortArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), shortValues);
            }
            else {
                throw new IllegalArgumentException("Type not supported in LongStreamReader");
            }
        }
        if (type == INTEGER || type == DATE) {
            int[] ints = new int[numFirstRows];
            for (int i = 0; i < numFirstRows; i++) {
                ints[i] = (int) values[i];
            }
            return new IntArrayBlock(numFirstRows,
                                     valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)), ints);
        }
        else if (type == BIGINT) {
            if (numFirstRows < numValues || values.length > (int) (numFirstRows * 1.2)) {
                return new LongArrayBlock(numFirstRows,
                                          valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)),
                                          Arrays.copyOf(values, numFirstRows));
            }
            Block block = new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
            values = null;
            valueIsNull = null;
            numValues = 0;
            return block;
        }
        else if (type == SMALLINT) {
            short[] shorts = new short[numFirstRows];
            for (int i = 0; i < numFirstRows; i++) {
                shorts[i] = (short) values[i];
            }
            return new ShortArrayBlock(numFirstRows,
                                       valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)), shorts);
        }
        else {
            throw new IllegalArgumentException("Type not supported by LongStreamReader");
        }
    }

    protected void ensureValuesCapacity()
    {
        if (!outputChannelSet) {
            return;
        }
        int capacity = numValues + inputQualifyingSet.getPositionCount();
        if (values == null || values.length < capacity) {
            values = resize(values, capacity);
        }
    }

    @Override
    public int getAverageResultSize()
    {
        return SIZE_OF_LONG;
    }
}

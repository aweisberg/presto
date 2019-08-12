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
package com.facebook.presto.operator;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.Session;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.operator.PagesRTreeIndex.GeometryWithPosition;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.operator.PagesSpatialIndex.EMPTY_INDEX;
import static io.airlift.units.DataSize.Unit.BYTE;

public class PagesSpatialIndexSupplier
        implements Supplier<PagesSpatialIndex>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesSpatialIndexSupplier.class).instanceSize();
    private static final int ENVELOPE_INSTANCE_SIZE = ClassLayout.parseClass(Envelope.class).instanceSize();
    private static final int STRTREE_INSTANCE_SIZE = ClassLayout.parseClass(STRtree.class).instanceSize();
    private static final int ABSTRACT_NODE_INSTANCE_SIZE = ClassLayout.parseClass(AbstractNode.class).instanceSize();

    private final Session session;
    private final LongArrayList addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final ObjectArrayList<Block>[] channels;
    private final Optional<Integer> radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final STRtree rtree;
    private final Map<Integer, Rectangle> partitions;
    private final long memorySizeInBytes;

    public PagesSpatialIndexSupplier(
            STRtree rtree,
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            ObjectArrayList<Block>[] channels,
            Optional<Integer> radiusChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions)
    {
        this.rtree = rtree;
        this.session = session;
        this.addresses = addresses;
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = channels;
        this.spatialRelationshipTest = spatialRelationshipTest;
        this.filterFunctionFactory = filterFunctionFactory;
        this.partitions = partitions;
        this.radiusChannel = radiusChannel;

        this.memorySizeInBytes = INSTANCE_SIZE +
                (rtree.isEmpty() ? 0 : STRTREE_INSTANCE_SIZE + computeMemorySizeInBytes(rtree.getRoot()));
    }

    private long computeMemorySizeInBytes(AbstractNode root)
    {
        if (root.getLevel() == 0) {
            return ABSTRACT_NODE_INSTANCE_SIZE + ENVELOPE_INSTANCE_SIZE + root.getChildBoundables().stream().mapToLong(child -> computeMemorySizeInBytes((ItemBoundable) child)).sum();
        }
        return ABSTRACT_NODE_INSTANCE_SIZE + ENVELOPE_INSTANCE_SIZE + root.getChildBoundables().stream().mapToLong(child -> computeMemorySizeInBytes((AbstractNode) child)).sum();
    }

    private long computeMemorySizeInBytes(ItemBoundable item)
    {
        return ENVELOPE_INSTANCE_SIZE + ((GeometryWithPosition) item.getItem()).getEstimatedMemorySizeInBytes();
    }

    // doesn't include memory used by channels and addresses which are shared with PagesIndex
    public DataSize getEstimatedSize()
    {
        return new DataSize(memorySizeInBytes, BYTE);
    }

    @Override
    public PagesSpatialIndex get()
    {
        if (rtree.isEmpty()) {
            return EMPTY_INDEX;
        }
        return new PagesRTreeIndex(session, addresses, types, outputChannels, channels, rtree, radiusChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }
}

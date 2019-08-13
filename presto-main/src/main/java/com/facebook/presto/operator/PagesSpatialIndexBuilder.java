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
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.geospatial.GeometryUtils.getJtsEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;

public class PagesSpatialIndexBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesSpatialIndexSupplier.class).instanceSize();
    private static final int ENVELOPE_INSTANCE_SIZE = ClassLayout.parseClass(Envelope.class).instanceSize();
    private static final int ITEM_BOUNDABLE_INSTANCE_SIZE = ClassLayout.parseClass(ItemBoundable.class).instanceSize();
    private static final int STRTREE_INSTANCE_SIZE = ClassLayout.parseClass(STRtree.class).instanceSize();

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
    private long memorySizeInBytes;
    private final Optional<Integer> partitionChannel;
    private final int geometryChannel;

    public PagesSpatialIndexBuilder(
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            ObjectArrayList<Block>[] channels,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions)
    {
        this.session = session;
        this.addresses = addresses;
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = channels;
        this.spatialRelationshipTest = spatialRelationshipTest;
        this.filterFunctionFactory = filterFunctionFactory;
        this.partitions = partitions;
        this.radiusChannel = radiusChannel;
        this.partitionChannel = partitionChannel;
        this.geometryChannel = geometryChannel;

        this.rtree = new STRtree();
        this.memorySizeInBytes = INSTANCE_SIZE + STRTREE_INSTANCE_SIZE;
    }

    public long addPage(int start)
    {
        Operator relateOperator = OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate);

        for (int position = start; position < addresses.size(); position++) {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            Block block = channels[geometryChannel].get(blockIndex);
            // TODO Consider pushing is-null and is-empty checks into a filter below the join
            if (block.isNull(blockPosition)) {
                continue;
            }

            Slice slice = block.getSlice(blockPosition, 0, block.getSliceLength(blockPosition));
            OGCGeometry ogcGeometry = deserialize(slice);
            verify(ogcGeometry != null);
            if (ogcGeometry.isEmpty()) {
                continue;
            }

            double radius = radiusChannel.map(channel -> DOUBLE.getDouble(channels[channel].get(blockIndex), blockPosition)).orElse(0.0);
            if (radius < 0) {
                continue;
            }

            if (!radiusChannel.isPresent()) {
                // If radiusChannel is supplied, this is a distance query, for which our acceleration won't help.
                accelerateGeometry(ogcGeometry, relateOperator);
            }

            int partition = -1;
            if (partitionChannel.isPresent()) {
                Block partitionBlock = channels[partitionChannel.get()].get(blockIndex);
                partition = toIntExact(INTEGER.getLong(partitionBlock, blockPosition));
            }

            Envelope envelope = getJtsEnvelope(ogcGeometry, radius);
            GeometryWithPosition geometryWithPosition = new GeometryWithPosition(ogcGeometry, partition, position);

            // Get as close as possible. Assumes 8-byte object refs
            memorySizeInBytes += ENVELOPE_INSTANCE_SIZE // Created by us, fixed size
                    + ITEM_BOUNDABLE_INSTANCE_SIZE //Created by STRTree
                    + geometryWithPosition.getEstimatedMemorySizeInBytes()
                    + 8; // 8-bytes for the internal array in STRTree
            rtree.insert(envelope, geometryWithPosition);
        }

        return memorySizeInBytes;
    }

    private static void accelerateGeometry(OGCGeometry ogcGeometry, Operator relateOperator)
    {
        // Recurse into GeometryCollections
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        while (true) {
            com.esri.core.geometry.Geometry esriGeometry = cursor.next();
            if (esriGeometry == null) {
                break;
            }
            relateOperator.accelerateGeometry(esriGeometry, null, Geometry.GeometryAccelerationDegree.enumMild);
        }
    }

    // doesn't include memory used by channels and addresses which are shared with PagesIndex
    public DataSize getEstimatedSize()
    {
        return new DataSize(memorySizeInBytes, BYTE);
    }

    public PagesSpatialIndexSupplier build()
    {
        rtree.build();
        return new PagesSpatialIndexSupplier(rtree, session, addresses, types, outputChannels, channels, radiusChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }
}

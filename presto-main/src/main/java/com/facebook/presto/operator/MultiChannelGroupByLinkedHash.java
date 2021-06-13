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

import com.facebook.presto.array.ByteBigArray;
import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.gen.JoinCompiler;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;

// This implementation assumes arrays used in the hash are always a power of 2
public final class MultiChannelGroupByLinkedHash
        extends MultiChannelGroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByLinkedHash.class).instanceSize();
    private static final float FILL_RATIO = 1.0f;

    private int[] buckets;

    private final IntBigArray prevGroupLink;
    private final ByteBigArray fastHashByteArray;

    public MultiChannelGroupByLinkedHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        super(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, updateMemory, FILL_RATIO);

        buckets = new int[hashCapacity];
        Arrays.fill(buckets, -1);
        prevGroupLink = new IntBigArray();
        prevGroupLink.ensureCapacity(maxFill);
        fastHashByteArray = new ByteBigArray();
        fastHashByteArray.ensureCapacity(maxFill);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(buckets) +
                groupAddressByGroupId.sizeOf() +
                prevGroupLink.sizeOf() +
                fastHashByteArray.sizeOf() +
                preallocatedMemoryInBytes;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        long rawHash = hashStrategy.hashRow(position, page);
        final int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for a slot containing this key
        int groupId = buckets[hashPosition];
        byte fastHash = (byte) rawHash;
        while (groupId != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByGroupId.get(groupId), groupId, position, page, fastHash, hashChannels)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            groupId = prevGroupLink.get(groupId);
        }

        return false;
    }

    protected int putIfAbsent(int position, Page page, long rawHash)
    {
        final int bucketId = (int) getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = buckets[bucketId];
        byte fastHash = (byte) rawHash;
        while (groupId != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByGroupId.get(groupId), groupId, position, page, fastHash, channels)) {
                // found an existing slot for this key
                break;
            }
            // increment position and mask to handle wrap around
            groupId = prevGroupLink.get(groupId);
            hashCollisions++;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(bucketId, position, page, rawHash);
        }
        return groupId;
    }

    @Override
    protected final int addNewGroup(int bucketId, int position, Page page, long rawHash)
    {
        // add the row to the open page
        for (int i = 0; i < channels.length; i++) {
            int hashChannel = channels[i];
            Type type = types.get(i);
            type.appendTo(page.getBlock(hashChannel), position, currentPageBuilder.getBlockBuilder(i));
        }
        if (precomputedHashChannel.isPresent()) {
            BIGINT.writeLong(currentPageBuilder.getBlockBuilder(precomputedHashChannel.getAsInt()), rawHash);
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        groupAddressByGroupId.set(groupId, address);
        prevGroupLink.set(groupId, buckets[bucketId]);
        buckets[bucketId] = groupId;
        fastHashByteArray.set(groupId, (byte) rawHash);

        // create new page builder if this page is full
        if (currentPageBuilder.isFull()) {
            startNewPage();
        }

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    protected boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for groupAddressByHash, rawHashByHashPosition, groupIdsByHash, and groupAddressByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES + Byte.BYTES) +
                (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES +
                currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        int newMask = newCapacity - 1;
        int[] newBuckets = new int[newCapacity];
        Arrays.fill(newBuckets, -1);

        for (int bucketId = 0; bucketId < buckets.length; bucketId++) {
            int curGroupId = buckets[bucketId];
            while (curGroupId != -1) {
                long address = groupAddressByGroupId.get(curGroupId);
                long rawHash = hashPosition(address);
                //long rawHash = rawHashArray.get(curGroupId);
                int newBucketId = (int) getHashPosition(rawHash, newMask);
                int oldPrevGroupId = prevGroupLink.get(curGroupId);
                int newPrevGroupId = newBuckets[newBucketId];
                prevGroupLink.set(curGroupId, newPrevGroupId);
                newBuckets[newBucketId] = curGroupId;
                curGroupId = oldPrevGroupId;
            }
        }

        this.mask = newMask;
        this.hashCapacity = newCapacity;
        this.maxFill = calculateMaxFill(newCapacity);
        this.buckets = newBuckets;
        groupAddressByGroupId.ensureCapacity(maxFill);
        prevGroupLink.ensureCapacity(maxFill);
        fastHashByteArray.ensureCapacity(maxFill);
        return true;
    }

    private boolean positionNotDistinctFromCurrentRow(long address, int groupId, int position, Page page, byte fastHash, int[] hashChannels)
    {
        //if (hashPosition(address) != rawHash) {
        if (fastHashByteArray.get(groupId) != fastHash) {
            return false;
        }
        seriousHashCollisions++;
        return hashStrategy.positionNotDistinctFromRow(decodeSliceIndex(address), decodePosition(address), position, page, hashChannels);
    }
}

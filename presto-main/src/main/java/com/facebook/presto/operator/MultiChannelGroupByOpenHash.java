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
public final class MultiChannelGroupByOpenHash
        extends MultiChannelGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByOpenHash.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private long[] groupAddressByHash;
    private int[] groupIdsByHash;
    private byte[] rawHashByHashPosition;

    public MultiChannelGroupByOpenHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        super(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, updateMemory, FILL_RATIO);

        groupAddressByHash = new long[hashCapacity];
        Arrays.fill(groupAddressByHash, -1);
        rawHashByHashPosition = new byte[hashCapacity];
        groupIdsByHash = new int[hashCapacity];
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupAddressByHash) +
                sizeOf(groupIdsByHash) +
                groupAddressByGroupId.sizeOf() +
                sizeOf(rawHashByHashPosition) +
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
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, hashChannels)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    @Override
    protected int putIfAbsent(int position, Page page, long rawHash)
    {
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, channels)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page, rawHash);
        }
        return groupId;
    }

    @Override
    protected int addNewGroup(int hashPosition, int position, Page page, long rawHash)
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

        groupAddressByHash[hashPosition] = address;
        rawHashByHashPosition[hashPosition] = (byte) rawHash;
        groupIdsByHash[hashPosition] = groupId;
        groupAddressByGroupId.set(groupId, address);

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
        long[] newKey = new long[newCapacity];
        byte[] rawHashes = new byte[newCapacity];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newCapacity];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (groupAddressByHash[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = groupAddressByHash[oldIndex];

            long rawHash = hashPosition(address);
            // find an empty slot for the address
            int pos = (int) getHashPosition(rawHash, newMask);
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newKey[pos] = address;
            rawHashes[pos] = (byte) rawHash;
            newValue[pos] = groupIdsByHash[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.hashCapacity = newCapacity;
        this.maxFill = calculateMaxFill(newCapacity);
        this.groupAddressByHash = newKey;
        this.rawHashByHashPosition = rawHashes;
        this.groupIdsByHash = newValue;
        groupAddressByGroupId.ensureCapacity(maxFill);
        return true;
    }

    @Override
    protected boolean positionNotDistinctFromCurrentRow(long address, int hashPosition, int position, Page page, byte rawHash, int[] hashChannels)
    {
        if (rawHashByHashPosition[hashPosition] != rawHash) {
            return false;
        }
        return hashStrategy.positionNotDistinctFromRow(decodeSliceIndex(address), decodePosition(address), position, page, hashChannels);
    }
}

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

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public abstract class MultiChannelGroupByHash
        implements GroupByHash
{
    //protected static final float FILL_RATIO = 0.75f;
    protected final float fillRatio;
    protected final List<Type> types;
    protected final List<Type> hashTypes;
    protected final int[] channels;

    protected final PagesHashStrategy hashStrategy;
    protected final List<ObjectArrayList<Block>> channelBuilders;
    protected final Optional<Integer> inputHashChannel;
    protected final HashGenerator hashGenerator;
    protected final OptionalInt precomputedHashChannel;
    protected final boolean processDictionary;
    protected PageBuilder currentPageBuilder;

    protected long completedPagesMemorySize;

    protected int hashCapacity;
    protected int maxFill;
    protected int mask;

    protected final LongBigArray groupAddressByGroupId;

    protected int nextGroupId;
    protected DictionaryLookBack dictionaryLookBack;
    protected long hashCollisions;
    protected long seriousHashCollisions;
    protected double expectedHashCollisions;

    // reserve enough memory before rehash
    protected final UpdateMemory updateMemory;
    protected long preallocatedMemoryInBytes;
    protected long currentPageSizeInBytes;

    public MultiChannelGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory,
            float fillRatio)
    {
        this.hashTypes = ImmutableList.copyOf(requireNonNull(hashTypes, "hashTypes is null"));

        requireNonNull(joinCompiler, "joinCompiler is null");
        requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : this.hashTypes;
        this.channels = hashChannels.clone();

        this.hashGenerator = inputHashChannel.isPresent() ? new PrecomputedHashGenerator(inputHashChannel.get()) : new InterpretedHashGenerator(this.hashTypes, hashChannels);
        this.processDictionary = processDictionary;
        this.fillRatio = fillRatio;

        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < hashChannels.length; i++) {
            outputChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        if (inputHashChannel.isPresent()) {
            this.precomputedHashChannel = OptionalInt.of(hashChannels.length);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        else {
            this.precomputedHashChannel = OptionalInt.empty();
        }
        this.channelBuilders = channelBuilders.build();
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(this.types, outputChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders, this.precomputedHashChannel);

        startNewPage();

        // reserve memory for the arrays
        hashCapacity = arraySize(expectedSize, fillRatio);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;

        groupAddressByGroupId = new LongBigArray();
        groupAddressByGroupId.ensureCapacity(maxFill);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public final long getRawHash(int groupId)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        return hashStrategy.hashPosition(blockIndex, position);
    }

    @Override
    public final long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public final long getSeriousHashCollisions()
    {
        return seriousHashCollisions;
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
    }

    @Override
    public final int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public final void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        hashStrategy.appendTo(blockIndex, position, pageBuilder, outputChannelOffset);
    }

    @Override
    public final Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new AddRunLengthEncodedPageWork(page);
        }
        if (canProcessDictionary(page)) {
            return new AddDictionaryPageWork(page);
        }

        return new AddNonDictionaryPageWork(page);
    }

    @Override
    public final Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new GetRunLengthEncodedGroupIdsWork(page);
        }
        if (canProcessDictionary(page)) {
            return new GetDictionaryGroupIdsWork(page);
        }

        return new GetNonDictionaryGroupIdsWork(page);
    }

    @VisibleForTesting
    @Override
    public final int getCapacity()
    {
        return hashCapacity;
    }

    private int putIfAbsent(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return putIfAbsent(position, page, rawHash);
    }

    protected abstract int putIfAbsent(int position, Page page, long rawHash);

    protected abstract int addNewGroup(int hashPosition, int position, Page page, long rawHash);

    protected final boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    protected final void startNewPage()
    {
        if (currentPageBuilder != null) {
            completedPagesMemorySize += currentPageBuilder.getRetainedSizeInBytes();
            currentPageBuilder = currentPageBuilder.newPageBuilderLike();
        }
        else {
            currentPageBuilder = new PageBuilder(types);
        }

        for (int i = 0; i < types.size(); i++) {
            channelBuilders.get(i).add(currentPageBuilder.getBlockBuilder(i));
        }
    }

    protected abstract boolean tryRehash();

    protected final long hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        if (precomputedHashChannel.isPresent()) {
            return getRawHash(sliceIndex, position);
        }
        return hashStrategy.hashPosition(sliceIndex, position);
    }

    protected final long getRawHash(int sliceIndex, int position)
    {
        return channelBuilders.get(precomputedHashChannel.getAsInt()).get(sliceIndex).getLong(position);
    }

    protected static long getHashPosition(long rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    protected final int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * fillRatio);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    // For a page that contains DictionaryBlocks, create a new page in which
    // the dictionaries from the DictionaryBlocks are extracted into the corresponding channels
    // From Page(DictionaryBlock1, DictionaryBlock2) create new page with Page(dictionary1, dictionary2)
    private Page createPageWithExtractedDictionary(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        Block dictionary = ((DictionaryBlock) page.getBlock(channels[0])).getDictionary();

        // extract data dictionary
        blocks[channels[0]] = dictionary;

        // extract hash dictionary
        if (inputHashChannel.isPresent()) {
            blocks[inputHashChannel.get()] = ((DictionaryBlock) page.getBlock(inputHashChannel.get())).getDictionary();
        }

        return new Page(dictionary.getPositionCount(), blocks);
    }

    private boolean canProcessDictionary(Page page)
    {
        if (!this.processDictionary || channels.length > 1 || !(page.getBlock(channels[0]) instanceof DictionaryBlock)) {
            return false;
        }

        if (inputHashChannel.isPresent()) {
            Block inputHashBlock = page.getBlock(inputHashChannel.get());
            DictionaryBlock inputDataBlock = (DictionaryBlock) page.getBlock(channels[0]);

            if (!(inputHashBlock instanceof DictionaryBlock)) {
                // data channel is dictionary encoded but hash channel is not
                return false;
            }
            if (!((DictionaryBlock) inputHashBlock).getDictionarySourceId().equals(inputDataBlock.getDictionarySourceId())) {
                // dictionarySourceIds of data block and hash block do not match
                return false;
            }
        }

        return true;
    }

    private boolean isRunLengthEncoded(Page page)
    {
        for (int i = 0; i < channels.length; i++) {
            if (!(page.getBlock(channels[i]) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    private int getGroupId(HashGenerator hashGenerator, Page page, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, page, hashGenerator.hashPosition(positionInDictionary, page));
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    private static final class DictionaryLookBack
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != -1;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }
    }

    private final class AddNonDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;

        private int lastPosition;

        public AddNonDictionaryPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // get the group for the current row
                putIfAbsent(lastPosition, page);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final class AddDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private int lastPosition;

        public AddDictionaryPageWork(Page page)
        {
            verify(canProcessDictionary(page), "invalid call to addDictionaryPage");
            this.page = requireNonNull(page, "page is null");
            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                getGroupId(hashGenerator, dictionaryPage, positionInDictionary);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final Page page;

        private boolean finished;

        public AddRunLengthEncodedPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (page.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(0, page);
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final class GetNonDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Page page;

        private boolean finished;
        private int lastPosition;

        public GetNonDictionaryGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, putIfAbsent(lastPosition, page));
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, blockBuilder.build());
        }
    }

    private final class GetDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            verify(canProcessDictionary(page), "invalid call to processDictionary");

            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);

            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                int groupId = getGroupId(hashGenerator, dictionaryPage, positionInDictionary);
                BIGINT.writeLong(blockBuilder, groupId);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, blockBuilder.build());
        }
    }

    private final class GetRunLengthEncodedGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final Page page;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (page.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(0, page);
            processFinished = true;
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            return new GroupByIdBlock(
                    nextGroupId,
                    new RunLengthEncodedBlock(
                            BIGINT.createFixedSizeBlockBuilder(1).writeLong(groupId).build(),
                            page.getPositionCount()));
        }
    }
}

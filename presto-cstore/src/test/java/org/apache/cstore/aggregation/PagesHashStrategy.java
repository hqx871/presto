//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.cstore.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.sql.gen.CompilerOperations;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.type.BigintOperators.BigintDistinctFromOperator.isDistinctFrom;

public final class PagesHashStrategy
        implements com.facebook.presto.operator.PagesHashStrategy
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(PagesHashStrategy.class).instanceSize();
    private long size;
    private final List<Block> channel0;
    private final List<Block> channel1;
    private final List<Block> channel2;
    private final List<Block> channel3;
    private final List<Block> joinChannel0;
    private final List<Block> joinChannel1;
    private final List<Block> joinChannel2;
    private final List<Block> hashChannel;

    public long hashRow(int position, Page blocks)
    {
        long result = 0L;
        result = result * 31L + (blocks.getBlock(0).isNull(position) ? 0L : VarcharType.VARCHAR.hash(blocks.getBlock(0), position));
        result = result * 31L + (blocks.getBlock(1).isNull(position) ? 0L : VarcharType.VARCHAR.hash(blocks.getBlock(1), position));
        result = result * 31L + (blocks.getBlock(2).isNull(position) ? 0L : VarcharType.VARCHAR.hash(blocks.getBlock(2), position));
        return result;
    }

    public int getChannelCount()
    {
        return 4;
    }

    public long getSizeInBytes()
    {
        return this.size;
    }

    public boolean isPositionNull(int blockIndex, int blockPosition)
    {
        if (((Block) this.joinChannel0.get(blockIndex)).isNull(blockPosition)) {
            return true;
        }
        else if (((Block) this.joinChannel1.get(blockIndex)).isNull(blockPosition)) {
            return true;
        }
        else {
            return ((Block) this.joinChannel2.get(blockIndex)).isNull(blockPosition);
        }
    }

    public long hashPosition(int blockIndex, int blockPosition)
    {
        if (this.hashChannel != null) {
            return BigintType.BIGINT.getLong((Block) this.hashChannel.get(blockIndex), blockPosition);
        }
        else {
            long result = 0L;
            result = result * 31L + (((Block) this.joinChannel0.get(blockIndex)).isNull(blockPosition) ? 0L : VarcharType.VARCHAR.hash((Block) this.joinChannel0.get(blockIndex), blockPosition));
            result = result * 31L + (((Block) this.joinChannel1.get(blockIndex)).isNull(blockPosition) ? 0L : VarcharType.VARCHAR.hash((Block) this.joinChannel1.get(blockIndex), blockPosition));
            result = result * 31L + (((Block) this.joinChannel2.get(blockIndex)).isNull(blockPosition) ? 0L : VarcharType.VARCHAR.hash((Block) this.joinChannel2.get(blockIndex), blockPosition));
            return result;
        }
    }

    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        if (!(leftPage.getBlock(0).isNull(leftPosition) | rightPage.getBlock(0).isNull(rightPosition) ? leftPage.getBlock(0).isNull(leftPosition) & rightPage.getBlock(0).isNull(rightPosition) : VarcharType.VARCHAR.equalTo(leftPage.getBlock(0), leftPosition, rightPage.getBlock(0), rightPosition))) {
            return false;
        }
        else if (!(leftPage.getBlock(1).isNull(leftPosition) | rightPage.getBlock(1).isNull(rightPosition) ? leftPage.getBlock(1).isNull(leftPosition) & rightPage.getBlock(1).isNull(rightPosition) : VarcharType.VARCHAR.equalTo(leftPage.getBlock(1), leftPosition, rightPage.getBlock(1), rightPosition))) {
            return false;
        }
        else {
            return leftPage.getBlock(2).isNull(leftPosition) | rightPage.getBlock(2).isNull(rightPosition) ? leftPage.getBlock(2).isNull(leftPosition) & rightPage.getBlock(2).isNull(rightPosition) : BigintType.BIGINT.equalTo(leftPage.getBlock(2), leftPosition, rightPage.getBlock(2), rightPosition);
        }
    }

    public boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftBlockPosition, int rightPosition, Page rightPage)
    {
        if (!VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(0), rightPosition)) {
            return false;
        }
        else if (!VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(1), rightPosition)) {
            return false;
        }
        else {
            return BigintType.BIGINT.equalTo((Block) this.joinChannel2.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(2), rightPosition);
        }
    }

    public boolean positionEqualsRow(int leftBlockIndex, int leftBlockPosition, int rightPosition, Page rightPage)
    {
        if (!(((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) | rightPage.getBlock(0).isNull(rightPosition) ? ((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) & rightPage.getBlock(0).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(0), rightPosition))) {
            return false;
        }
        else if (!(((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) | rightPage.getBlock(1).isNull(rightPosition) ? ((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) & rightPage.getBlock(1).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(1), rightPosition))) {
            return false;
        }
        else {
            return ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) | rightPage.getBlock(2).isNull(rightPosition) ? ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) & rightPage.getBlock(2).isNull(rightPosition) : BigintType.BIGINT.equalTo((Block) this.joinChannel2.get(leftBlockIndex), leftBlockPosition, rightPage.getBlock(2), rightPosition);
        }
    }

    public boolean positionEqualsRow(int leftBlockIndex, int leftBlockPosition, int rightPosition, Page page, int[] rightChannels)
    {
        if (((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) | page.getBlock(rightChannels[0]).isNull(rightPosition) ? ((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) & page.getBlock(rightChannels[0]).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, page.getBlock(rightChannels[0]), rightPosition)) {
            if (((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) | page.getBlock(rightChannels[1]).isNull(rightPosition) ? ((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) & page.getBlock(rightChannels[1]).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, page.getBlock(rightChannels[1]), rightPosition)) {
                return ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) | page.getBlock(rightChannels[2]).isNull(rightPosition) ? ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) & page.getBlock(rightChannels[2]).isNull(rightPosition) : BigintType.BIGINT.equalTo((Block) this.joinChannel2.get(leftBlockIndex), leftBlockPosition, page.getBlock(rightChannels[2]), rightPosition);
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    public boolean positionNotDistinctFromRow(int leftBlockIndex, int leftBlockPosition, int rightPosition, Page page, int[] rightChannels)
    {
        //boolean wasNull = false;
        if (this.joinChannel0.get(leftBlockIndex).isNull(leftBlockPosition) || page.getBlock(rightChannels[0]).isNull(rightPosition) ? ((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) && page.getBlock(rightChannels[0]).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, page.getBlock(rightChannels[0]), rightPosition)) {
            if (this.joinChannel1.get(leftBlockIndex).isNull(leftBlockPosition) || page.getBlock(rightChannels[1]).isNull(rightPosition) ? ((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) & page.getBlock(rightChannels[1]).isNull(rightPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, page.getBlock(rightChannels[1]), rightPosition)) {
                Block var10000 = this.joinChannel2.get(leftBlockIndex);
                return !isDistinctFrom(var10000, leftBlockPosition, page.getBlock(rightChannels[2]), rightPosition);
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    public boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        if (!VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel0.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }
        else if (!VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel1.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }
        else {
            return BigintType.BIGINT.equalTo((Block) this.joinChannel2.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel2.get(rightBlockIndex), rightBlockPosition);
        }
    }

    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        if (!(((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) | ((Block) this.joinChannel0.get(rightBlockIndex)).isNull(rightBlockPosition) ? ((Block) this.joinChannel0.get(leftBlockIndex)).isNull(leftBlockPosition) & ((Block) this.joinChannel0.get(rightBlockIndex)).isNull(rightBlockPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel0.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel0.get(rightBlockIndex), rightBlockPosition))) {
            return false;
        }
        else if (!(((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) | ((Block) this.joinChannel1.get(rightBlockIndex)).isNull(rightBlockPosition) ? ((Block) this.joinChannel1.get(leftBlockIndex)).isNull(leftBlockPosition) & ((Block) this.joinChannel1.get(rightBlockIndex)).isNull(rightBlockPosition) : VarcharType.VARCHAR.equalTo((Block) this.joinChannel1.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel1.get(rightBlockIndex), rightBlockPosition))) {
            return false;
        }
        else {
            return ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) | ((Block) this.joinChannel2.get(rightBlockIndex)).isNull(rightBlockPosition) ? ((Block) this.joinChannel2.get(leftBlockIndex)).isNull(leftBlockPosition) & ((Block) this.joinChannel2.get(rightBlockIndex)).isNull(rightBlockPosition) : BigintType.BIGINT.equalTo((Block) this.joinChannel2.get(leftBlockIndex), leftBlockPosition, (Block) this.joinChannel2.get(rightBlockIndex), rightBlockPosition);
        }
    }

    public int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isSortChannelPositionNull(int blockIndex, int blockPosition)
    {
        throw new UnsupportedOperationException();
    }

    public PagesHashStrategy(List<List<Block>> channels, OptionalInt hashChannel)
    {
        this.size = INSTANCE_SIZE;
        this.channel0 = (List) channels.get(0);

        int blockIndex;
        for (blockIndex = 0; CompilerOperations.lessThan(blockIndex, ((List) channels.get(0)).size()); ++blockIndex) {
            this.size += ((Block) ((List) channels.get(0)).get(blockIndex)).getRetainedSizeInBytes();
        }

        this.channel1 = (List) channels.get(1);

        for (blockIndex = 0; CompilerOperations.lessThan(blockIndex, ((List) channels.get(1)).size()); ++blockIndex) {
            this.size += ((Block) ((List) channels.get(1)).get(blockIndex)).getRetainedSizeInBytes();
        }

        this.channel2 = (List) channels.get(2);

        for (blockIndex = 0; CompilerOperations.lessThan(blockIndex, ((List) channels.get(2)).size()); ++blockIndex) {
            this.size += ((Block) ((List) channels.get(2)).get(blockIndex)).getRetainedSizeInBytes();
        }

        this.channel3 = (List) channels.get(3);

        for (blockIndex = 0; CompilerOperations.lessThan(blockIndex, ((List) channels.get(3)).size()); ++blockIndex) {
            this.size += ((Block) ((List) channels.get(3)).get(blockIndex)).getRetainedSizeInBytes();
        }

        this.joinChannel0 = (List) channels.get(0);
        this.joinChannel1 = (List) channels.get(1);
        this.joinChannel2 = (List) channels.get(2);
        if (hashChannel.isPresent()) {
            this.hashChannel = (List) channels.get(hashChannel.getAsInt());
        }
        else {
            this.hashChannel = null;
        }
    }

    public void appendTo(int blockIndex, int blockPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        VarcharType.VARCHAR.appendTo(this.channel0.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 0));
        VarcharType.VARCHAR.appendTo((Block) this.channel1.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 1));
        BigintType.BIGINT.appendTo((Block) this.channel2.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 2));
        BigintType.BIGINT.appendTo((Block) this.channel3.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 3));
    }
}

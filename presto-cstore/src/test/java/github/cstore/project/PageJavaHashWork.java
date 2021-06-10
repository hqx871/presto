//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package github.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.Arrays;
import java.util.List;

public final class PageJavaHashWork
        implements Work<List<Block>>
{
    private List<BlockBuilder> blockBuilders;
    private SqlFunctionProperties properties;
    private Page page;
    private SelectedPositions selectedPositions;
    private int nextIndexOrPosition;
    private List<Block> result;
    private long[] hashArray = new long[3];

    public void evaluate(SqlFunctionProperties properties, Page page, int position)
    {
        Block block0 = page.getBlock(3);
        Block block1 = page.getBlock(4);
        Block block2 = page.getBlock(5);
        boolean wasNull = block0.isNull(position) || block1.isNull(position) || block2.isNull(position);
        BlockBuilder temp0 = this.blockBuilders.get(0);
        if (wasNull) {
            temp0.appendNull();
        }
        else {
            hashArray[0] = VarcharType.VARCHAR.getSlice(block0, position).hashCode();
            hashArray[1] = VarcharType.VARCHAR.getSlice(block1, position).hashCode();
            hashArray[2] = BigintType.BIGINT.getLong(block2, position);
            long hash = Arrays.hashCode(hashArray);
            BigintType.BIGINT.writeLong(temp0, hash);
        }
    }

    public PageJavaHashWork(List<BlockBuilder> blockBuilders, SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions)
    {
        this.blockBuilders = ImmutableList.copyOf(blockBuilders);
        this.properties = properties;
        this.page = page;
        this.selectedPositions = selectedPositions;
        this.nextIndexOrPosition = selectedPositions.getOffset();
        this.result = null;
    }

    public List<Block> getResult()
    {
        return this.result;
    }

    public boolean process()
    {
        int from = this.nextIndexOrPosition;
        int to = this.selectedPositions.getOffset() + this.selectedPositions.size();
        int index;
        if (this.selectedPositions.isList()) {
            int[] positions = this.selectedPositions.getPositions();

            for (index = from; index < to; ++index) {
                this.evaluate(this.properties, this.page, positions[index]);
            }
        }
        else {
            for (index = from; index < to; ++index) {
                this.evaluate(this.properties, this.page, index);
            }
        }

        Builder<Block> blocksBuilder = ImmutableList.builder();

        for (int temp0 = 0; temp0 < 1; ++temp0) {
            blocksBuilder.add(this.blockBuilders.get(temp0).build());
        }

        this.result = blocksBuilder.build();
        return true;
    }
}

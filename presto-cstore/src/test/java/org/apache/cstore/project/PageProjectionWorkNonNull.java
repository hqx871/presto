package org.apache.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import javax.annotation.Nullable;

import java.util.List;

/**
 * input blocks:
 * l_tax,l_discount
 * output blocks:
 * l_extendedprice * (1 - l_discount)
 * l_extendedprice * (1 - l_discount) * (1 + l_tax)
 */
public final class PageProjectionWorkNonNull
        implements Work<List<Block>>
{
    private final List<BlockBuilder> blockBuilders;
    private final SqlFunctionProperties properties;
    private final Page page;
    private final SelectedPositions selectedPositions;
    private final int nextIndexOrPosition;
    private List<Block> result;

    private final Block block0;
    private final Block block1;
    private final Block block2;

    private final BlockBuilder builder0;
    private final BlockBuilder builder1;

    private double getCse(int position)
    {
        double var10001 = DoubleType.DOUBLE.getDouble(block0, position);
        double var10003 = DoubleType.DOUBLE.getDouble(block1, position);
        double var10002 = (1.0D - var10003);
        return var10001 * var10002;
    }

    public boolean process()
    {
        int from = this.nextIndexOrPosition;
        int to = this.selectedPositions.getOffset() + this.selectedPositions.size();
        int index;
        if (this.selectedPositions.isList()) {
            int[] positions = this.selectedPositions.getPositions();

            for (index = from; index < to; ++index) {
                this.evaluate(positions[index]);
            }
        }
        else {
            for (index = from; index < to; ++index) {
                this.evaluate(index);
            }
        }

        Builder<Block> blocksBuilder = ImmutableList.builder();

        for (int temp0 = 0; temp0 < 2; ++temp0) {
            blocksBuilder.add(this.blockBuilders.get(temp0).build());
        }

        this.result = blocksBuilder.build();
        return true;
    }

    public void evaluate(int position)
    {
        double cseResult = this.getCse(position);
        DoubleType.DOUBLE.writeDouble(builder0, cseResult);
        double var10002 = DoubleType.DOUBLE.getDouble(block2, position);
        double var10001 = cseResult * (1.0D + var10002);
        DoubleType.DOUBLE.writeDouble(builder1, var10001);
    }

    public PageProjectionWorkNonNull(List<BlockBuilder> blockBuilders, @Nullable SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions)
    {
        this.blockBuilders = ImmutableList.copyOf(blockBuilders);
        this.properties = properties;
        this.page = page;
        this.selectedPositions = selectedPositions;
        this.nextIndexOrPosition = selectedPositions.getOffset();
        this.result = null;

        this.block0 = page.getBlock(0);
        this.block1 = page.getBlock(1);
        this.block2 = page.getBlock(2);

        this.builder0 = blockBuilders.get(0);
        this.builder1 = blockBuilders.get(1);
    }

    public List<Block> getResult()
    {
        return this.result;
    }
}

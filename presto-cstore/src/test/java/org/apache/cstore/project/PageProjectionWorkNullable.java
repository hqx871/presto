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
public final class PageProjectionWorkNullable
        implements Work<List<Block>>
{
    private final List<BlockBuilder> blockBuilders;
    private final SqlFunctionProperties properties;
    private final Page page;
    private final SelectedPositions selectedPositions;
    private final int nextIndexOrPosition;
    private List<Block> result;

    private double multiply$cseResult;
    private boolean multiply$cseIsNull;

    private final Block block_0;
    private final Block block_1;
    private final Block block_2;

    private final BlockBuilder builder_0;
    private final BlockBuilder builder_1;

    private void getmultiply$cse(int position)
    {
        if (block_0.isNull(position) || block_1.isNull(position)) {
            this.multiply$cseIsNull = true;
        }
        else {
            double var10001 = DoubleType.DOUBLE.getDouble(block_0, position);
            double var10003 = DoubleType.DOUBLE.getDouble(block_1, position);
            double var10002 = (1.0D - var10003);
            this.multiply$cseIsNull = false;
            this.multiply$cseResult = var10001 * var10002;
        }
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

        for (int temp_0 = 0; temp_0 < 2; ++temp_0) {
            blocksBuilder.add(this.blockBuilders.get(temp_0).build());
        }

        this.result = blocksBuilder.build();
        return true;
    }

    public void evaluate(int position)
    {
        this.getmultiply$cse(position);
        if (multiply$cseIsNull) {
            builder_0.appendNull();
        }
        else {
            DoubleType.DOUBLE.writeDouble(builder_0, multiply$cseResult);
        }

        if (multiply$cseIsNull || block_2.isNull(position)) {
            builder_1.appendNull();
        }
        else {
            double var10002 = DoubleType.DOUBLE.getDouble(block_2, position);
            double var10001 = multiply$cseResult * (1.0D + var10002);
            DoubleType.DOUBLE.writeDouble(builder_1, var10001);
        }
    }

    public PageProjectionWorkNullable(List<BlockBuilder> blockBuilders, @Nullable SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions)
    {
        this.blockBuilders = ImmutableList.copyOf(blockBuilders);
        this.properties = properties;
        this.page = page;
        this.selectedPositions = selectedPositions;
        this.nextIndexOrPosition = selectedPositions.getOffset();
        this.result = null;

        this.block_0 = page.getBlock(0);
        this.block_1 = page.getBlock(1);
        this.block_2 = page.getBlock(2);

        this.builder_0 = blockBuilders.get(0);
        this.builder_1 = blockBuilders.get(1);
    }

    public List<Block> getResult()
    {
        return this.result;
    }
}

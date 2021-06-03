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
public final class PageProjectionWorkDemo
        implements Work<List<Block>>
{
    private final List<BlockBuilder> blockBuilders;
    private final SqlFunctionProperties properties;
    private final Page page;
    private final SelectedPositions selectedPositions;
    private final int nextIndexOrPosition;
    private List<Block> result;
    private boolean multiply$cseEvaluated;
    private Double multiply$cseResult;

    private Double getmultiply$cse(@Nullable SqlFunctionProperties properties, Page page, int position)
    {
        Block block_0 = page.getBlock(0);
        Block block_1 = page.getBlock(1);
        boolean wasNull = false;
        if (!this.multiply$cseEvaluated) {
            double var10001;
            if (block_0.isNull(position)) {
                wasNull = true;
                var10001 = 0.0D;
            }
            else {
                var10001 = DoubleType.DOUBLE.getDouble(block_0, position);
            }

            if (wasNull) {
                var10001 = 0.0D;
            }
            else {
                double var10002;
                if (wasNull) {
                    var10002 = 0.0D;
                }
                else {
                    double var10003;
                    if (block_1.isNull(position)) {
                        wasNull = true;
                        var10003 = 0.0D;
                    }
                    else {
                        var10003 = DoubleType.DOUBLE.getDouble(block_1, position);
                    }

                    var10002 = wasNull ? 0.0D : (1.0D - var10003);
                }

                var10001 = wasNull ? 0.0D : (var10001 * var10002);
            }

            this.multiply$cseResult = wasNull ? null : var10001;
            this.multiply$cseEvaluated = true;
        }

        return this.multiply$cseResult;
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

        for (int temp_0 = 0; temp_0 < 2; ++temp_0) {
            blocksBuilder.add(this.blockBuilders.get(temp_0).build());
        }

        this.result = blocksBuilder.build();
        return true;
    }

    public void evaluate(SqlFunctionProperties properties, Page page, int position)
    {
        Block block_2 = page.getBlock(1);
        boolean wasNull = false;
        this.multiply$cseEvaluated = false;
        BlockBuilder temp_0 = this.blockBuilders.get(0);
        Double var10000 = this.getmultiply$cse(properties, page, position);
        double var11;
        if (var10000 == null) {
            wasNull = true;
            var11 = 0.0D;
        }
        else {
            var11 = var10000;
        }

        if (wasNull) {
            temp_0.appendNull();
        }
        else {
            double temp_1 = var11;
            DoubleType.DOUBLE.writeDouble(temp_0, temp_1);
        }

        wasNull = false;
        temp_0 = this.blockBuilders.get(0);
        var10000 = this.getmultiply$cse(properties, page, position);
        if (var10000 == null) {
            wasNull = true;
            var11 = 0.0D;
        }
        else {
            var11 = var10000;
        }

        if (wasNull) {
            var11 = 0.0D;
        }
        else {
            double var10001;
            if (wasNull) {
                var10001 = 0.0D;
            }
            else {
                double var10002;
                if (block_2.isNull(position)) {
                    wasNull = true;
                    var10002 = 0.0D;
                }
                else {
                    var10002 = DoubleType.DOUBLE.getDouble(block_2, position);
                }

                var10001 = wasNull ? 0.0D : 1.0D + var10002;
            }

            var11 = wasNull ? 0.0D : var11 * var10001;
        }

        if (wasNull) {
            temp_0.appendNull();
        }
        else {
            double temp_3 = var11;
            DoubleType.DOUBLE.writeDouble(temp_0, temp_3);
        }

        wasNull = false;
    }

    public PageProjectionWorkDemo(List<BlockBuilder> blockBuilders, @Nullable SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions)
    {
        this.blockBuilders = ImmutableList.copyOf(blockBuilders);
        this.properties = properties;
        this.page = page;
        this.selectedPositions = selectedPositions;
        this.nextIndexOrPosition = selectedPositions.getOffset();
        this.result = null;
        this.multiply$cseEvaluated = false;
        this.multiply$cseResult = null;
    }

    public List<Block> getResult()
    {
        return this.result;
    }
}

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.cstore.project;

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
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public final class PageHashWork
        implements Work<List<Block>>
{
    private List<BlockBuilder> blockBuilders;
    private SqlFunctionProperties properties;
    private Page page;
    private SelectedPositions selectedPositions;
    private int nextIndexOrPosition;
    private List<Block> result;

    public void evaluate(SqlFunctionProperties properties, Page page, int position)
    {
        Block block0 = page.getBlock(0);
        Block block1 = page.getBlock(1);
        Block block2 = page.getBlock(2);
        boolean wasNull = false;
        BlockBuilder temp0 = this.blockBuilders.get(0);
        long var10000;
        Slice var10001;
        long var11;
        if (wasNull) {
            var10000 = 0L;
        }
        else {
            if (block0.isNull(position)) {
                wasNull = true;
                var10001 = null;
            }
            else {
                var10001 = VarcharType.VARCHAR.getSlice(block0, position);
            }

            var11 = wasNull ? 0L : var10001.hashCode();
            if (wasNull) {
                wasNull = false;
                var11 = 0L;
            }

            var10000 = wasNull ? 0L : Objects.hash(0L, var11);
        }

        if (wasNull) {
            var10000 = 0L;
        }
        else {
            if (block1.isNull(position)) {
                wasNull = true;
                var10001 = null;
            }
            else {
                var10001 = VarcharType.VARCHAR.getSlice(block1, position);
            }

            var11 = wasNull ? 0L : var10001.hashCode();
            if (wasNull) {
                wasNull = false;
                var11 = 0L;
            }

            var10000 = wasNull ? 0L : Objects.hash(var10000, var11);
        }

        if (wasNull) {
            var10000 = 0L;
        }
        else {
            if (block2.isNull(position)) {
                wasNull = true;
                var11 = 0L;
            }
            else {
                var11 = BigintType.BIGINT.getLong(block2, position);
            }

            var11 = wasNull ? 0L : Objects.hash(var11);
            if (wasNull) {
                wasNull = false;
                var11 = 0L;
            }

            var10000 = wasNull ? 0L : Objects.hash(var10000, var11);
        }

        if (wasNull) {
            temp0.appendNull();
        }
        else {
            long temp1 = var10000;
            BigintType.BIGINT.writeLong(temp0, temp1);
        }

        wasNull = false;
    }

    public PageHashWork(List<BlockBuilder> blockBuilders, SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions)
    {
        this.blockBuilders = ImmutableList.copyOf((Collection) blockBuilders);
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

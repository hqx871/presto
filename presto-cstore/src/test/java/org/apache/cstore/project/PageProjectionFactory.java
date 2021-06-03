package org.apache.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;

import javax.annotation.Nullable;

import java.util.List;

public interface PageProjectionFactory
{
    Work<List<Block>> create(List<BlockBuilder> blockBuilders, @Nullable SqlFunctionProperties properties, Page page, SelectedPositions selectedPositions);
}

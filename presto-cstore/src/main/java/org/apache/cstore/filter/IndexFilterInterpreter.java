package org.apache.cstore.filter;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.CStoreSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;
import org.apache.cstore.SelectedPositions;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.StringEncodedColumnReader;
import org.apache.cstore.manage.CStoreDatabase;
import org.apache.cstore.meta.TableMeta;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class IndexFilterInterpreter
{
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;
    private final CStoreDatabase database;
    private final CStoreSplit split;
    private final TableMeta tableMeta;

    public IndexFilterInterpreter(TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session,
            CStoreDatabase database,
            CStoreSplit split)
    {
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.session = session;
        this.database = database;
        this.split = split;
        this.tableMeta = database.getTableMeta(split.getSchema(), split.getTable());
    }

    public Iterator<SelectedPositions> compute(RowExpression filter, int rowCount, int vectorSize)
    {
        if (filter != null) {
            Bitmap bitmap = filter.accept(new Visitor(), new VisitorContext());
            BitmapIterator bitmapIterator = bitmap.iterator();
            int[] selections = new int[vectorSize];
            return new Iterator<SelectedPositions>()
            {
                @Override
                public boolean hasNext()
                {
                    return bitmapIterator.hasNext();
                }

                @Override
                public SelectedPositions next()
                {
                    int size = bitmapIterator.next(selections);
                    return SelectedPositions.positionsList(selections, 0, size);
                }
            };
        }
        else {
            return new Iterator<SelectedPositions>()
            {
                int position;

                @Override
                public boolean hasNext()
                {
                    return position < rowCount;
                }

                @Override
                public SelectedPositions next()
                {
                    int offset = position;
                    int size = Math.min(vectorSize, rowCount - position);
                    position += size;
                    return SelectedPositions.positionsRange(offset, size);
                }
            };
        }
    }

    private class VisitorContext
    {
        public Bitmap getBitmap(InputReferenceExpression input, Object value)
        {
            //todo
            return null;
        }
    }

    private class Visitor
            implements RowExpressionVisitor<Bitmap, VisitorContext>
    {
        @Override
        public Bitmap visitCall(CallExpression call, VisitorContext context)
        {
            FunctionHandle functionHandle = call.getFunctionHandle();
            if (standardFunctionResolution.isNotFunction(functionHandle)) {
                Bitmap bitmap = call.getArguments().get(0).accept(this, context);
                return bitmap.not();
            }
            FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
            Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
            if (operatorTypeOptional.isPresent()) {
                switch (operatorTypeOptional.get()) {
                    case EQUAL: {
                        VariableReferenceExpression field = (VariableReferenceExpression) call.getArguments().get(0);
                        BitmapColumnReader bitmapReader = database.getBitmapReader(split.getSchema(), split.getTable(), field.getName());
                        StringEncodedColumnReader stringReader = (StringEncodedColumnReader) database.getColumnReader(split.getSchema(),
                                split.getTable(), field.getName(), VarcharType.VARCHAR);
                        String value = ((Slice) ((ConstantExpression) call.getArguments().get(1)).getValue()).toStringUtf8();
                        int id = stringReader.decode(value);
                        return bitmapReader.readObject(id);
                    }
                    default:
                }
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitInputReference(InputReferenceExpression reference, VisitorContext context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitConstant(ConstantExpression node, VisitorContext context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitLambda(LambdaDefinitionExpression lambda, VisitorContext context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitVariableReference(VariableReferenceExpression reference, VisitorContext context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitSpecialForm(SpecialFormExpression node, VisitorContext context)
        {
            switch (node.getForm()) {
                case IN: {
                    checkArgument(node.getArguments().size() >= 2, "values must not be empty");
                    InputReferenceExpression input =
                            (InputReferenceExpression) node.getArguments().get(0);
                    List<RowExpression> valueExpressions = node.getArguments().subList(1, node.getArguments().size());
                    List<Object> values = valueExpressions.stream().map(value -> ((ConstantExpression) value).getValue()).collect(toList());
                    List<Bitmap> bitmaps = values.stream().map(value -> context.getBitmap(input, value)).collect(toList());
                    return bitmaps.size() == 1 ? bitmaps.get(0) : bitmaps.get(0).and(bitmaps.subList(1, bitmaps.size()));
                }
                case AND: {
                    List<Bitmap> bitmaps = node.getArguments().stream().map(value -> value.accept(this, context)).collect(toList());
                    if (bitmaps.isEmpty()) {
                        return null;
                    }
                    else {
                        return bitmaps.get(0).and(bitmaps.subList(1, bitmaps.size()));
                    }
                }
            }
            throw new UnsupportedOperationException();
        }
    }
}

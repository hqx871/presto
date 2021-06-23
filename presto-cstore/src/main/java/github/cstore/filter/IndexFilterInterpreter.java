package github.cstore.filter;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeManager;
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
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapFactory;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.StringEncodedColumnReader;
import io.airlift.slice.Slice;

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
    private final BitmapFactory bitmapFactory;

    public IndexFilterInterpreter(TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            BitmapFactory bitmapFactory)
    {
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.bitmapFactory = bitmapFactory;
    }

    public Iterator<SelectedPositions> compute(RowExpression filter, int rowCount, int vectorSize, Context context)
    {
        if (filter != null) {
            Bitmap bitmap = filter.accept(new Visitor(), context);
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

    public abstract static class Context
    {
        public abstract CStoreColumnReader getColumnReader(String column);

        public abstract BitmapColumnReader getBitmapReader(String column);

        public Bitmap getBitmap(VariableReferenceExpression field, ConstantExpression valueExpr)
        {
            BitmapColumnReader bitmapReader = getBitmapReader(field.getName());
            StringEncodedColumnReader stringReader = (StringEncodedColumnReader) getColumnReader(field.getName());
            String value = ((Slice) valueExpr.getValue()).toStringUtf8();
            int id = stringReader.decode(value); //todo long dict encoded?
            return bitmapReader.readObject(id);
        }

        public List<Bitmap> getBitmap(VariableReferenceExpression field, List<ConstantExpression> valueExpr)
        {
            BitmapColumnReader bitmapReader = getBitmapReader(field.getName());
            StringEncodedColumnReader stringReader = (StringEncodedColumnReader) getColumnReader(field.getName());
            List<String> values = valueExpr.stream().map(v -> ((Slice) v.getValue()).toStringUtf8()).collect(toList());
            return values.stream().map(stringReader::decode).map(bitmapReader::readObject).collect(toList());
        }
    }

    private class Visitor
            implements RowExpressionVisitor<Bitmap, Context>
    {
        @Override
        public Bitmap visitCall(CallExpression call, Context context)
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
                        ConstantExpression value = (ConstantExpression) call.getArguments().get(1);
                        return context.getBitmap(field, value);
                    }
                    default:
                }
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitInputReference(InputReferenceExpression reference, Context context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitConstant(ConstantExpression node, Context context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bitmap visitSpecialForm(SpecialFormExpression node, Context context)
        {
            switch (node.getForm()) {
                case IN: {
                    checkArgument(node.getArguments().size() >= 2, "values must not be empty");
                    VariableReferenceExpression input =
                            (VariableReferenceExpression) node.getArguments().get(0);
                    List<RowExpression> valueExpressions = node.getArguments().subList(1, node.getArguments().size());
                    List<ConstantExpression> values = valueExpressions.stream().map(value -> ((ConstantExpression) value)).collect(toList());
                    List<Bitmap> bitmaps = context.getBitmap(input, values);
                    //return bitmaps.size() == 1 ? bitmaps.get(0) : bitmaps.get(0).or(bitmaps.subList(1, bitmaps.size()));
                    return bitmapFactory.or(bitmaps);
                }
                case AND: {
                    List<Bitmap> bitmaps = node.getArguments().stream().map(value -> value.accept(this, context)).collect(toList());
                    //return bitmaps.get(0).and(bitmaps.subList(1, bitmaps.size()));
                    return bitmapFactory.and(bitmaps);
                }
                case OR: {
                    List<Bitmap> bitmaps = node.getArguments().stream().map(value -> value.accept(this, context)).collect(toList());
                    //return bitmaps.get(0).or(bitmaps.subList(1, bitmaps.size()));
                    return bitmapFactory.or(bitmaps);
                }
                default:
            }
            throw new UnsupportedOperationException();
        }
    }
}

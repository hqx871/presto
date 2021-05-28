package org.apache.cstore.filter;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.apache.cstore.bitmap.Bitmap;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class IndexFilterInterpreter
{
    public Bitmap compute(RowExpression filter)
    {
        return filter.accept(new Visitor(), new VisitorContext());
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

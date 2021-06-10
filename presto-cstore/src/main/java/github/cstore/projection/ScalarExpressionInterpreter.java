package github.cstore.projection;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

public class ScalarExpressionInterpreter
        implements RowExpressionVisitor<ScalarCall, ScalarExpressionInterpreter.Context>
{
    @Override
    public ScalarCall visitCall(CallExpression call, Context context)
    {
        return null;
    }

    @Override
    public ScalarCall visitInputReference(InputReferenceExpression reference, Context context)
    {
        return null;
    }

    @Override
    public ScalarCall visitConstant(ConstantExpression literal, Context context)
    {
        return null;
    }

    @Override
    public ScalarCall visitLambda(LambdaDefinitionExpression lambda, Context context)
    {
        return null;
    }

    @Override
    public ScalarCall visitVariableReference(VariableReferenceExpression reference, Context context)
    {
        return null;
    }

    @Override
    public ScalarCall visitSpecialForm(SpecialFormExpression specialForm, Context context)
    {
        return null;
    }

    public static class Context
    {
    }
}

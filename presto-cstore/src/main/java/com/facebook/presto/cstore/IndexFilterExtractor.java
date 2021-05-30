/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cstore;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
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

import java.util.Optional;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static java.util.Objects.requireNonNull;

public class IndexFilterExtractor
        implements RowExpressionVisitor<RowExpression, IndexFilterExtractor.Context>
{
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;

    public IndexFilterExtractor(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session)
    {
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.session = requireNonNull(session, "session is null");
    }

    public abstract static class Context
    {
        public abstract boolean hasBitmapIndex(String column);
    }

    public RowExpression convert(RowExpression filter, Context context)
    {
        return filter.accept(this, context);
    }

    @Override
    public RowExpression visitCall(CallExpression call, Context context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            call.getArguments().get(0).accept(this, context);
            return call;
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            switch (operatorType) {
                case EQUAL: {
                    if (call.getArguments().get(0) instanceof VariableReferenceExpression
                            && call.getArguments().get(1) instanceof ConstantExpression
                            && context.hasBitmapIndex(((VariableReferenceExpression) call.getArguments().get(0)).getName())) {
                        return call;
                    }
                }
                default:
            }
        }
        throw new PrestoException(CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Function " + call + " not supported in IndexFilter filter");
    }

    @Override
    public RowExpression visitInputReference(InputReferenceExpression reference, Context context)
    {
        throw new PrestoException(CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "IndexFilter does not support struct dereference: " + reference);
    }

    @Override
    public RowExpression visitConstant(ConstantExpression literal, Context context)
    {
        throw new PrestoException(CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "IndexFilter does not support constant: ");
    }

    @Override
    public RowExpression visitLambda(LambdaDefinitionExpression lambda, Context context)
    {
        throw new PrestoException(CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "IndexFilter does not support lambda: " + lambda);
    }

    @Override
    public RowExpression visitVariableReference(VariableReferenceExpression reference, Context context)
    {
        return reference;
    }

    @Override
    public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Context context)
    {
        switch (specialForm.getForm()) {
            case IN: {
                RowExpression field = specialForm.getArguments().get(0);
                if (field instanceof VariableReferenceExpression
                        && isConstantList(specialForm.getArguments().subList(1, specialForm.getArguments().size()))
                        && context.hasBitmapIndex(((VariableReferenceExpression) field).getName())) {
                    return specialForm;
                }
                break;
            }
            case AND:
            case OR: {
                for (RowExpression arg : specialForm.getArguments()) {
                    arg.accept(this, context);
                }
                return specialForm;
            }
            default:
        }
        throw new PrestoException(CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "IndexFilter does not support special form: " + specialForm);
    }

    private static boolean isConstantList(Iterable<RowExpression> values)
    {
        for (RowExpression value : values) {
            if (!(value instanceof ConstantExpression)) {
                return false;
            }
        }
        return true;
    }
}

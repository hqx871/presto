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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.cstore.CStoreDatabase;
import org.apache.cstore.filter.IndexFilterExtractor;
import org.apache.cstore.meta.TableMeta;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_QUERY_GENERATOR_FAILURE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CStorePlanOptimizer
        implements ConnectorPlanOptimizer
{
    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public CStorePlanOptimizer(
            CStoreDatabase database,
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.database = database;
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standard function resolution is null");
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                standardFunctionResolution,
                functionMetadataManager);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return maxSubplan.accept(new Visitor(session, idAllocator), null);
    }

    private static Optional<CStoreTableHandle> getCStoreTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof CStoreTableHandle) {
                return Optional.of((CStoreTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        for (int i = 0; i < node.getSources().size(); i++) {
            if (children.get(i) != node.getSources().get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final IdentityHashMap<PlanNodeId, PlanNode> visitedNodeMap = new IdentityHashMap<>();

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
        }

        private Optional<PlanNode> tryCreatingNewScanNode(FilterNode plan, TableScanNode tableScanNode)
        {
            CStoreTableHandle tableHandle = getCStoreTableHandle(tableScanNode).orElseThrow(() -> new PrestoException(CSTORE_QUERY_GENERATOR_FAILURE, "Expected to find a cstore table handle"));
            TableHandle oldTableHandle = tableScanNode.getTable();
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new CStoreTableHandle(tableHandle.getSchema(), tableHandle.getTable(), plan.getPredicate()),
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());
            return Optional.of(
                    new TableScanNode(
                            idAllocator.getNextId(),
                            newTableHandle,
                            plan.getOutputVariables(),
                            tableScanNode.getAssignments(),
                            tableScanNode.getCurrentConstraint(),
                            tableScanNode.getEnforcedConstraint()));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> newChildren = node.getSources().stream().map(source -> source.accept(this, context)).collect(toImmutableList());
            return replaceChildren(node, newChildren);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (visitedNodeMap.containsKey(node.getId())) {
                return visitedNodeMap.get(node.getId());
            }
            if (!(node.getSource() instanceof TableScanNode) ||
                    !(((TableScanNode) node.getSource()).getTable().getConnectorHandle() instanceof CStoreTableHandle)) {
                PlanNode newPlan = this.visitPlan(node, context);
                visitedNodeMap.put(newPlan.getId(), newPlan);
                return newPlan;
            }
            TableScanNode tableScanNode = (TableScanNode) node.getSource();
            CStoreTableHandle tableHandle = (CStoreTableHandle) tableScanNode.getTable().getConnectorHandle();
            List<RowExpression> pushable = new ArrayList<>();
            List<RowExpression> nonPushable = new ArrayList<>();
            IndexFilterExtractor indexFilterExpressionConverter = new IndexFilterExtractor(typeManager, functionMetadataManager, standardFunctionResolution, session);
            TableMeta tableMeta = database.getTableMeta(tableHandle.getSchema(), tableHandle.getTable());
            IndexFilterExtractor.Context extractorContext = new IndexFilterExtractor.Context()
            {
                @Override
                public boolean hasBitmapIndex(String column)
                {
                    return tableMeta.getBitmap(column) != null;
                }
            };
            for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(node.getPredicate())) {
                try {
                    pushable.add(indexFilterExpressionConverter.convert(conjunct, extractorContext));
                }
                catch (PrestoException e) {
                    nonPushable.add(conjunct);
                }
            }
            if (!pushable.isEmpty()) {
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), logicalRowExpressions.combineConjuncts(pushable));
                Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, logicalRowExpressions.combineConjuncts(nonPushable)));

                Optional<PlanNode> scanFilterNode = tryCreatingNewScanNode(pushableFilter, tableScanNode);
                if (scanFilterNode.isPresent()) {
                    visitedNodeMap.put(pushableFilter.getId(), pushableFilter);
                    if (nonPushableFilter.isPresent()) {
                        FilterNode nonPushableFilterNode = nonPushableFilter.get();
                        nonPushableFilterNode.replaceChildren(ImmutableList.of(scanFilterNode.get()));
                        visitedNodeMap.put(nonPushableFilterNode.getId(), nonPushableFilterNode);
                        return nonPushableFilterNode;
                    }
                    else {
                        return scanFilterNode.get();
                    }
                }
                else {
                    visitedNodeMap.put(node.getId(), node);
                    return node;
                }
            }
            visitedNodeMap.put(node.getId(), node);
            return node;
        }
    }
}

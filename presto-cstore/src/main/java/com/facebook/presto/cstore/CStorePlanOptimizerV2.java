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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.google.inject.Inject;
import org.apache.cstore.CStoreDatabase;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CStorePlanOptimizerV2
        extends CStorePlanOptimizer
{
    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public CStorePlanOptimizerV2(
            CStoreDatabase database,
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        super(database, typeManager, determinismEvaluator, functionMetadataManager, standardFunctionResolution);
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
        @Deprecated
        private final IdentityHashMap<PlanNodeId, PlanNode> visitedNodeMap = new IdentityHashMap<>();
        private final IdentityHashMap<PlanNodeId, PlanNode> visitedNodeState = new IdentityHashMap<>();

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            if (visitedNodeState.containsKey(node.getId())) {
                return node;
            }
            visitedNodeState.put(node.getId(), node);
            return visitChildren(node, context);
        }

        private PlanNode visitChildren(PlanNode node, Void context)
        {
            List<PlanNode> newChildren = new ArrayList<>(node.getSources().size());
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, context);
                newChildren.add(newChild);
                if (newChild != child) {
                    changed = true;
                }
            }
            return changed ? node.replaceChildren(newChildren) : node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            if (visitedNodeState.containsKey(node.getId())) {
                return node;
            }
            visitedNodeState.put(node.getId(), node);
            Optional<CStoreTableHandle> tableHandle = getCStoreTableHandle(node);
            if (!tableHandle.isPresent()) {
                return node;
            }
            TableScanNode newScan = createNewScanNode(node, tableHandle.get(), node, context);
            visitedNodeState.put(newScan.getId(), newScan);
            return newScan;
        }

        private TableScanNode createNewScanNode(TableScanNode scanNode, CStoreTableHandle tableHandle, PlanNode query, Void context)
        {
            TableHandle oldTableHandle = scanNode.getTable();
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new CStoreTableHandleV2(tableHandle.getSchema(), tableHandle.getTable(), query),
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());
            //todo am id right?
            return new TableScanNode(
                    idAllocator.getNextId(),
                    newTableHandle,
                    scanNode.getOutputVariables(),
                    scanNode.getAssignments(),
                    scanNode.getCurrentConstraint(),
                    scanNode.getEnforcedConstraint());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            return visitPushableNode(node, context);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            return visitPushableNode(node, context);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() == AggregationNode.Step.PARTIAL) {
                return visitPushableNode(node, context);
            }
            else {
                return visitPlan(node, context);
            }
        }

        public PlanNode visitPushableNode(PlanNode node, Void context)
        {
            if (visitedNodeState.containsKey(node.getId())) {
                return node;
            }
            visitedNodeState.put(node.getId(), node);
            PlanNode newNode = visitChildren(node, context);

            if (newNode.getSources().size() != 1 || !(newNode.getSources().get(0) instanceof TableScanNode)) {
                return node;
            }

            TableScanNode tableScanNode = (TableScanNode) newNode.getSources().get(0);
            Optional<CStoreTableHandle> tableHandle = getCStoreTableHandle(tableScanNode);
            if (!tableHandle.isPresent()) {
                return newNode;
            }
            TableScanNode newScan = createNewScanNode(tableScanNode, tableHandle.get(), newNode, context);
            visitedNodeState.put(newScan.getId(), newScan);
            return newScan;
        }
    }
}

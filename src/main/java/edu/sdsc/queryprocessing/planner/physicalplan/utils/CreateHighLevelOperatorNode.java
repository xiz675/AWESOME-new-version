package edu.sdsc.queryprocessing.planner.physicalplan.utils;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.BiOpePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.FilterPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.ReducePhysical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
// todo: add local varIDs when create the nodes
public class CreateHighLevelOperatorNode {
    public static List<PhysicalOperator> getNodes(HighLevelOperator oprt, Map<Integer, Integer> old2New, boolean pipeline) {
        List<PhysicalOperator> result = new ArrayList<>();
        PhysicalOperator physical;
        if (oprt instanceof MapExpression)  {
            MapExpression temp = (MapExpression) oprt;
            physical = new MapPhysical(temp.getMappedVarID(), old2New.get(temp.getInputOperator()), temp.getInputVar(), temp.getLocalVarID(), temp.getElementType(), pipeline);
        }
        else if (oprt instanceof FilterExpression) {
            FilterExpression temp = (FilterExpression) oprt;
            // todo: add the same. Then the only part needed to be changed is the logical part from parsing
            // todo: should set the applied Var type
            physical = new FilterPhysical(temp.getFilteredVarID(), old2New.get(temp.getInputOperator()), temp.getInputVar(), temp.getLocalVarID(), temp.getAppliedVarType(), temp.getElementType());
        }
        else if (oprt instanceof BiPredicate) {
            BiPredicate temp = (BiPredicate) oprt;
            physical = new BiOpePhysical(temp.getBiOpe(), old2New.get(temp.getLeftOpeID()), old2New.get(temp.getRightOpeID()));
        }
        else {
            assert oprt instanceof ReduceExpression;
            ReduceExpression temp = (ReduceExpression) oprt;
            physical = new ReducePhysical(temp.getReducedVarID(), old2New.get(temp.getInputOperator()), temp.getInputVar());
        }
        physical.setOutputVar(PlanUtils.physicalVar(oprt.getOutputVar()));
        result.add(physical);
        return result;
    }
}

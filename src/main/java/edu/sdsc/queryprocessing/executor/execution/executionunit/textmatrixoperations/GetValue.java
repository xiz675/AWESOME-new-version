//package edu.sdsc.queryprocessing.executor.execution.executionunit.textmatrixoperations;
//
//import edu.sdsc.datatype.execution.TextMatrix;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.GetTextMatrixValuePhysical;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//// this is only for  vector which only has one row or column
//public class GetValue extends BlockExecutionBase {
//    public double execute(TextMatrix tm, Integer index) {
//        return tm.getValue().get(index);
//    }
//    private Integer index;
//    private TextMatrix tm;
//
//    public GetValue(GetTextMatrixValuePhysical opt, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        if (opt.isHasIndex()) {
//            index = opt.getIndex();
//        }
//        else{
//            index = ((AwesomeInteger) getTableEntryWithLocal(opt.getIndexID(), evt, localEvt)).getValue();
//        }
//        tm = (TextMatrix) getTableEntryWithLocal(opt.getTmID(), evt, localEvt).getValue();
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        double value = tm.getValue().get(index-1);
//        return new AwesomeDouble(value);
//    }
//}

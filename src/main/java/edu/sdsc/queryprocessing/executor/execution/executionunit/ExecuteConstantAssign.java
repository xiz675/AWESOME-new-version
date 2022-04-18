//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.ConstantAssignmentPhysical;
//
//import java.util.List;
//
//public class ExecuteConstantAssign extends BlockExecutionBase {
//    private Object value;
//    public ExecuteConstantAssign(ConstantAssignmentPhysical ope) {
//        value  = ope.getValue();
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        if (value instanceof Boolean) {
//            return new AwesomeBoolean((boolean) value);
//        }
//        else if (value instanceof Double) {
//            return new AwesomeDouble((double) value);
//        }
//        else if (value instanceof Integer) {
//            return new AwesomeInteger((Integer) value);
//        }
//        else if (value instanceof String) {
//            return new AwesomeString((String) value);
//        }
//        else {
//            // todo: it is not needed now but may be needed in the future
//            assert value instanceof List;
//            return new MaterializedList<Object>((List<Object>) value);
//        }
//    }
//}

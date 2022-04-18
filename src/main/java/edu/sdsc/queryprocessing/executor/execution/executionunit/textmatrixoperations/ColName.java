//package edu.sdsc.queryprocessing.executor.execution.executionunit.textmatrixoperations;
//
//import edu.sdsc.datatype.execution.FunctionParameter;
//import edu.sdsc.datatype.execution.TextMatrix;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
//import edu.sdsc.utils.Pair;
//
//import javax.json.JsonArray;
//
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
//
//public class ColName extends BlockExecutionBase {
//    private FunctionPhysicalOperator ope;
//    private ExecutionVariableTable evt;
//
//    public ColName(FunctionPhysicalOperator ope, ExecutionVariableTable evt) {
//        this.ope = ope;
//        this.evt = evt;
//    }
//
//    @Override
//    public MaterializedList execute() {
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        JsonArray parameter = ope.getParameters();
//        List<FunctionParameter> parameters = translateParameters(parameter);
//        Integer textMatrixID = parameters.get(0).getVarID();
//        TextMatrix matrix = (TextMatrix) evt.getTableEntry(new Pair<>(textMatrixID, "*")).getValue();
//        return new MaterializedList(matrix.getColMapping());
////        this.evt.insertEntry(ouptVar, new MaterializedList<>(x));
//    }
//}

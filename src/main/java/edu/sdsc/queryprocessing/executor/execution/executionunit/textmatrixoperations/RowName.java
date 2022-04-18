//package edu.sdsc.queryprocessing.executor.execution.executionunit.textmatrixoperations;
//
//import edu.sdsc.datatype.execution.FunctionParameter;
//import edu.sdsc.datatype.execution.TextMatrix;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.RowNamesPhysical;
//import edu.sdsc.utils.Pair;
//
//import javax.json.JsonArray;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class RowName extends BlockExecutionBase {
//    private FunctionPhysicalOperator ope;
//    private ExecutionVariableTable evt;
//    private ExecutionVariableTable[] localEvt;
//
//    public RowName(RowNamesPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        this.ope = ope;
//        this.evt = evt;
//        this.localEvt = localEvt;
//    }
//
//    @Override
//    public MaterializedList execute() {
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        JsonArray parameter = ope.getParameters();
//        List<FunctionParameter> parameters = translateParameters(parameter);
//        Integer textMatrixID = parameters.get(0).getVarID();
//        TextMatrix matrix = (TextMatrix) getTableEntryWithLocal(new Pair<>(textMatrixID, "*"), evt, localEvt).getValue();
//        return new MaterializedList(matrix.getRowMappinp());
////        this.evt.insertEntry(ouptVar, new MaterializedList<>(x));
//    }
//}

//package edu.sdsc.queryprocessing.executor.execution.executionunit.highleveloperations;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.BiOpePhysical;
//import edu.sdsc.variables.logicalvariables.DataTypeEnum;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//
//import javax.json.JsonObject;
//
//import java.sql.Connection;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.executor.utils.ExecutionUtil.parallelExecutionUnit;
////import static edu.sdsc.queryprocessing.executor.execution.mainexecution.ExecutionQuery.unitExecution;
//
//// todo: currently it only support compare of Integers, should support double or maybe String. Should add double
//public class BiOperationExecutor {
//    private PhysicalOperator leftOpe;
//    private PhysicalOperator rightOpe;
//    private String biOpe;
//    private DataTypeEnum resultType;
//    private ExecutionVariableTable evt;
//    private ExecutionVariableTable[] localEvt;
//    private VariableTable vt;
//    private JsonObject config;
//    private Connection sqlCon;
//    private List<PhysicalOperator> operators;
//
//    // in the main execution code, need to find the operators to be passed here
//    // there is no input variable for this operator. Just call the sub operators and compare the result
//    public BiOperationExecutor(BiOpePhysical ope, PhysicalOperator leftOpe, PhysicalOperator rightOpe, List<PhysicalOperator> g, ExecutionVariableTable evt, VariableTable vt, JsonObject config, Connection sqlCon, ExecutionVariableTable... localEvt) {
//        this.biOpe = ope.getBiOperation();
//        this.leftOpe = leftOpe;
//        this.rightOpe = rightOpe;
//        this.evt = evt;
//        this.vt = vt;
//        this.config = config;
//        this.localEvt = localEvt;
//        this.sqlCon = sqlCon;
//        this.operators = g;
////        this.resultType = ope.getElementType();
//    }
//
//
//    public ExecutionTableEntry executeWithMode() {
//        return this.execute();
//    }
//
//
//    // there is no local variable or collection input, just call the subOperators and compare
//    public AwesomeBoolean execute() {
//        ExecutionTableEntry leftResult = parallelExecutionUnit(1, leftOpe, operators,  evt, config, vt, sqlCon, false, localEvt);
//        ExecutionTableEntry rightResult = parallelExecutionUnit(1, rightOpe, operators, evt, config, vt, sqlCon, false, localEvt);
//        boolean result;
////        assert resultType.equals(DataTypeEnum.Integer) || resultType.equals(DataTypeEnum.Double);
//        // todo: currently make it just double or integer and add more later
//        double left = getDoubleValue(leftResult.getValue());
//        double right = getDoubleValue(rightResult.getValue());
//        if (biOpe.equals(">")) {
//            result = left > right;
//        }
//        else if (biOpe.equals("<")) {
//            result = left < right;
//        }
//        else {
//            assert biOpe.equals("=");
//            result = (left == right);
//        }
//        return new AwesomeBoolean(result);
//    }
//
//    private static double getDoubleValue(Object x) {
//        if (x instanceof Double) {
//            return (Double) x;
//        }
//        else {
//            assert x instanceof Integer;
//            return Double.valueOf((Integer) x);
//        }
//
//    }
//
//    public static void main(String[] args) {
//        Double x = 2.0;
//        Integer y = 3;
//        Double ss = Double.valueOf(x);
//        Double z = Double.valueOf(y);
//        System.out.println(ss < z);
//    }
//
//
//}

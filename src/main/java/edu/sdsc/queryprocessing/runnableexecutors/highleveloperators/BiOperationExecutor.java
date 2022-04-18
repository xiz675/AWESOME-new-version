package edu.sdsc.queryprocessing.runnableexecutors.highleveloperators;


import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.BiOpePhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;

import java.sql.Connection;
import java.util.List;

//import static edu.sdsc.queryprocessing.executor.execution.mainexecution.ExecutionQuery.unitExecution;

// todo: currently it only support compare of Integers, should support double or maybe String. Should add double
public class BiOperationExecutor extends AwesomeBlockRunnable<Pair<PhysicalOperator, PhysicalOperator>, Boolean> {
    private String biOpe;
    private ExecutionVariableTable evt;
    private ExecutionVariableTable[] localEvt;
    private VariableTable vt;
    private JsonObject config;
    private Connection sqlCon;

    // in the main execution code, need to find the operators to be passed here
    // there is no input variable for this operator. Just call the sub operators and compare the result
    public BiOperationExecutor(BiOpePhysical ope, PhysicalOperator leftOpe, PhysicalOperator rightOpe, ExecutionVariableTable evt, VariableTable vt, JsonObject config, Connection sqlCon, ExecutionVariableTable... localEvt) {
        super(new Pair<>(leftOpe, rightOpe));
        this.biOpe = ope.getBiOperation();
        this.evt = evt;
        this.vt = vt;
        this.config = config;
        this.localEvt = localEvt;
        this.sqlCon = sqlCon;
//        this.resultType = ope.getElementType();
    }



    private static double getDoubleValue(Object x) {
        if (x instanceof Double) {
            return (Double) x;
        }
        else {
            assert x instanceof Integer;
            return Double.valueOf((Integer) x);
        }

    }

    public static void main(String[] args) {
        Double x = 2.0;
        Integer y = 3;
        Double ss = Double.valueOf(x);
        Double z = Double.valueOf(y);
        System.out.println(ss < z);
    }

    @Override
    public void executeBlocking() {
        Pair<PhysicalOperator, PhysicalOperator> input = getMaterializedInput();
        AwesomeRunnable exeLeft = input.first.createExecutor(evt, config, vt, sqlCon, false, null, false, localEvt);
        AwesomeRunnable exeRight = input.second.createExecutor(evt, config, vt, sqlCon, false, null, false, localEvt);
        exeLeft.run();
        exeRight.run();
//        ExecutionTableEntry leftResult = exeLeft.createTableEntry();
//        ExecutionTableEntry rightResult = exeRight.createTableEntry();
        boolean result;
//        assert resultType.equals(DataTypeEnum.Integer) || resultType.equals(DataTypeEnum.Double);
        // todo: currently make it just double or integer and add more later
        double left = (Double) exeLeft.createTableEntry().getValue();
        double right = (Double) exeRight.createTableEntry().getValue() ;
        if (biOpe.equals(">")) {
            result = left > right;
        }
        else if (biOpe.equals("<")) {
            result = left < right;
        }
        else {
            assert biOpe.equals("=");
            result = (left == right);
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeBoolean(this.getMaterializedOutput());
    }
}


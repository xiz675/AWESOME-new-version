package edu.sdsc.queryprocessing.runnableexecutors.highleveloperators;

import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.FilterPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.ExecutionUtil.createTableEntryForLocal;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
import static edu.sdsc.utils.TextMatrixUtils.*;

// todo: assume that Filter is blocking for now. Should be able to be a pipeline operator. Should add Text later
// there can be two types of applied variables. One is a collection same as other HLOs, and the other one is (Text) matrix which can be iterated in two ways

public class FilterExecutor extends AwesomeBlockRunnable<FilterPhysical, ExecutionTableEntry> {
    //    private StreamHLCollection streamTEs;
    private MaterializedHLCollection listTEs;
    //    private StreamList<Object> streamElement;
    private MaterializedList<Object> listData;
    //    private StreamHLCollection streamResult;
    private AwesomeTextMatrix textMatrixData;
    private boolean isTE = false;
    private Pair<Integer, String> localVar;
    private ExecutionVariableTable evt;
    private VariableTable vt;
    private PhysicalOperator innerOpe;
    private JsonObject config;
    private DataTypeEnum elementType;
    private DataTypeEnum appliedVarType;
    private List<PhysicalOperator> physicalGraph;
    private ExecutionVariableTable localEvtFromParent = null;
    private Connection sqlCon;

    // todo: add other execution methods later, should support stream of HL Collection or list as input
    // todo: find the inner operator node from graph by its id

    // for Filter, also add element type to physical operator or maybe logical one
    public FilterExecutor(FilterPhysical ope, ExecutionVariableTable evt, VariableTable vt, JsonObject config, Connection sqlCon, ExecutionVariableTable... localEvt) {
        // assign the value based on thr type of the table entry
        // there is only one input operator
        super(ope);
        this.innerOpe = ope.getInnerOpe();
//        this.innerOpe = innerOpe;
//        this.physicalGraph = g;
        this.config = config;
        this.sqlCon = sqlCon;
        this.elementType = ope.getElementType();
        Pair<Integer, String> inputVar = new Pair<>(ope.getAppliedVarID(), "*");
        ExecutionTableEntry input = getTableEntryWithLocal(inputVar, evt, localEvt);
//        ExecutionTableEntry input = evt.getTableEntry(inputVar);
        this.localVar = new Pair<>(ope.getLocalVarID().get(0), "*");
        this.evt = evt;
        this.vt = vt;
        this.appliedVarType = ope.getAppliedVarType();
        if (localEvt.length > 0) {
            this.localEvtFromParent = localEvt[0];
        }
        if (this.appliedVarType.equals(DataTypeEnum.TextMatrix)) {
            this.textMatrixData = (AwesomeTextMatrix) input;
        }
//        if (input instanceof AwesomeTextMatrix) {
//            this.textMatrixData = (AwesomeTextMatrix) input;
//        }
        else {
            if (input instanceof MaterializedHLCollection) {
                this.listTEs = (MaterializedHLCollection) input;
                this.isTE = true;
            }
            else {
                assert input instanceof MaterializedList;
                this.listData = (MaterializedList) input;
            }
        }
    }


//
//    public ExecutionTableEntry executeWithMode() {
//
//    }

    // todo: how to deal with filtering columns of a matrix
    private AwesomeTextMatrix executeTextMatrix() {
        TextMatrix input = this.textMatrixData.getValue();
        List rawRowNames = input.getRowMapping();
        List rawColNames = input.getColMapping();
        List<Object> rowNames = new ArrayList<>();
        List<Object> colNames = new ArrayList<>();
        if (this.elementType.equals(DataTypeEnum.MatrixColumn)) {
            // create TE for each column
            // if iterate throw column, then the row names should remain the same
            rowNames = rawRowNames;
            List<double[]> value = new ArrayList<>();
            for (int colIndex=0; colIndex<input.getColNum(); colIndex++) {
                // todo: change it later
                TextMatrix perCol = getCol(input, colIndex);
                ExecutionTableEntry crtTE = createTableEntryForLocal(DataTypeEnum.TextMatrix, perCol);
                ExecutionVariableTable localEvt = new ExecutionVariableTable();
                localEvt.insertEntry(this.localVar, crtTE);
//                ExecutionVariableTable[] localEvtsArray = addToArray(localEvts, localEvt);
//                this.evt.insertEntry(this.localVar, crtTE);
                AwesomeRunnable executor;
                ExecutionTableEntry boolValue;
                // todo: change this later
                if (this.localEvtFromParent != null) {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false, this.localEvtFromParent, localEvt);
                }
                else {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false,  localEvt);
                }
                executor.run();
//                boolValue = new AwesomeBoolean((Boolean) executor.getMaterializedOutput());
//                if (innerOpe instanceof HighLevelPhysical) {
//                    boolValue = hloExecution((HighLevelPhysical) innerOpe, vt, config, evt, physicalGraph);
//                }
//                else {
//                    boolValue = unitExecution(innerOpe, vt, config, evt);
//                }
                if ((Boolean) executor.getMaterializedOutput()) {
                    colNames.add(rawColNames.get(colIndex));
                    value.add(perCol.getValue().getData());
                }
            }
            return new AwesomeTextMatrix(createTextMatrixFromColumns(rowNames, colNames, value));
        }
        else {
            assert this.elementType.equals(DataTypeEnum.MatrixRow);
            colNames = rawColNames;
            List<double[]> value = new ArrayList<>();
            for (int rowIndex=0; rowIndex<input.getRowNum(); rowIndex++) {
                TextMatrix perRow = getRow(input, rowIndex);
                ExecutionTableEntry crtTE = createTableEntryForLocal(DataTypeEnum.TextMatrix, perRow);
                ExecutionVariableTable localEvt = new ExecutionVariableTable();
                localEvt.insertEntry(this.localVar, crtTE);
//                ExecutionVariableTable[] localEvtsArray = addToArray(localEvts, localEvt);
//                this.evt.insertEntry(this.localVar, crtTE);
                AwesomeRunnable executor;
                if (this.localEvtFromParent != null) {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false, this.localEvtFromParent, localEvt);
                }
                else {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false,  localEvt);
                }
                executor.run();
                if ((Boolean) executor.getMaterializedOutput()) {
                    rowNames.add(rawRowNames.get(rowIndex));
                    value.add(perRow.getValue().getData());
                }
            }
            return new AwesomeTextMatrix(createTextMatrixFromRows(rowNames, colNames, value));
        }
    }



    // for any type of input, needs to determine if this is a TE or not
    // the sub-operator should always return a AwesomeBoolean Type and execute create or append the TE which has value true
    private MaterializedHLCollection executeCollection() {
        List<ExecutionTableEntry> result = new ArrayList<>();
        if (isTE) {
            List<ExecutionTableEntry> TEs = this.listTEs.getValue();
            for (ExecutionTableEntry i : TEs) {
                ExecutionVariableTable localEvt = new ExecutionVariableTable();
                localEvt.insertEntry(this.localVar, i);
                AwesomeRunnable executor;
                if (this.localEvtFromParent != null) {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false, this.localEvtFromParent, localEvt);
                }
                else {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false,  localEvt);
                }
                executor.run();
                if ((Boolean) executor.getMaterializedOutput()) {
                    result.add(i);
                }
            }
        }
        else {
            List<Object> listReslt = this.listData.getValue();
            for (Object i : listReslt) {
                ExecutionTableEntry crtTE = createTableEntryForLocal(elementType, i);
                ExecutionVariableTable localEvt = new ExecutionVariableTable();
                localEvt.insertEntry(this.localVar, crtTE);
                AwesomeRunnable executor;
                if (this.localEvtFromParent != null) {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false, this.localEvtFromParent, localEvt);
                }
                else {
                    executor = innerOpe.createExecutor(evt, config, vt, sqlCon, false, null, false,  localEvt);
                }
                executor.run();
                if ((Boolean) executor.getMaterializedOutput()) {
                    result.add(crtTE);
                }
            }
        }
        return new MaterializedHLCollection(result);
    }


    public static ExecutionVariableTable[] addToArray(List<ExecutionVariableTable> evts, ExecutionVariableTable evt) {
        evts.add(evt);
        ExecutionVariableTable[] evtsArray = new ExecutionVariableTable[evts.size()];
        for (int i=0; i<evts.size(); i++) {
            evtsArray[i] = evts.get(i);
        }
        return evtsArray;
    }

    public static void main(String[] args) {

    }

    @Override
    public void executeBlocking() {
        ExecutionTableEntry result;
        long startTime;
        long endTime;
        if (this.appliedVarType.equals(DataTypeEnum.TextMatrix)){
            startTime = System.currentTimeMillis();
            result = this.executeTextMatrix();
            endTime = System.currentTimeMillis();
            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
        }
        else {
            startTime = System.currentTimeMillis();
            result = this.executeCollection();
            endTime = System.currentTimeMillis();
            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return getMaterializedOutput();
    }
}

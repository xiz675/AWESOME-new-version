package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.RowNamesPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;

import javax.json.JsonArray;
import java.util.List;

public class RowName extends AwesomeBlockRunnable<TextMatrix, List> {
    private FunctionPhysicalOperator ope;
    private ExecutionVariableTable evt;
    private ExecutionVariableTable[] localEvt;

    public RowName(TextMatrix matrix) {
        super(matrix);
    }

    @Override
    public void executeBlocking() {
        setMaterializedOutput(getMaterializedInput().getRowMapping());
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedList(this.getMaterializedOutput());
    }

}

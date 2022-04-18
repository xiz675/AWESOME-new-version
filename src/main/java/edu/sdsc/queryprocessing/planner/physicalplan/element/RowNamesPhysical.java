package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.RowNames;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.RowName;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark: finish assign parameters
public class RowNamesPhysical extends FunctionPhysicalOperator{
    private Pair<Integer, String> matrixID;

    public RowNamesPhysical(RowNames rn) {
        this.setParameters(rn.getParameters());
        List<FunctionParameter> parameters = translateParameters(rn.getParameters());
        matrixID = new Pair<>(parameters.get(0).getVarID(), "*");
        this.setInputVar(PlanUtils.physicalVar(rn.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(rn.getOutputVar()));
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        JsonArray parameter = this.getParameters();
        List<FunctionParameter> parameters = translateParameters(parameter);
        Integer textMatrixID = parameters.get(0).getVarID();
        return new RowName((TextMatrix) getTableEntryWithLocal(new Pair<>(textMatrixID, "*"), evt, localEvt).getValue());
    }
}
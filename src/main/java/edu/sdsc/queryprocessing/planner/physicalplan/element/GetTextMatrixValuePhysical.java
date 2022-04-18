package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeInteger;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.GetTextMatrixValue;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.GetValue;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark: finish assigning at physical level. If this is a constant then it is done, if not, then need to search
// execution
public class GetTextMatrixValuePhysical extends FunctionPhysicalOperator {
    private boolean hasIndex = false;
    private Integer index = null;
    private Pair<Integer, String> indexID;
    private Pair<Integer, String> tmID;

    public GetTextMatrixValuePhysical(GetTextMatrixValue ope) {
        this.setParameters(ope.getParameters());
        // translate parameters to assign values
        List<FunctionParameter> parameters = translateParameters(ope.getParameters());
        tmID = new Pair<>(parameters.get(0).getVarID(), "*");
        // the index can be a constant value or a variable which needs to be searched
        FunctionParameter in = parameters.get(1);
        if (in.isHasValue()) {hasIndex = true; index = (Integer) in.getValue();}
        else {indexID = new Pair<>(parameters.get(1).getVarID(), "*");}
        // assign input and output variables for making plan
        this.setInputVar(PlanUtils.physicalVar(ope.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(ope.getOutputVar()));
    }

    public void setTmID(Pair<Integer, String> tmID) {
        this.tmID = tmID;
    }

    public Pair<Integer, String> getTmID() {
        return tmID;
    }

    public boolean isHasIndex() {
        return hasIndex;
    }

    public Integer getIndex() {
        return index;
    }

    public Pair<Integer, String> getIndexID() {
        return indexID;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        Integer index;
        if (this.isHasIndex()) {
            index = this.getIndex();
        }
        else{
            index = ((AwesomeInteger) getTableEntryWithLocal(this.getIndexID(), evt, localEvt)).getValue();
        }
        TextMatrix input = (TextMatrix) getTableEntryWithLocal(this.getTmID(), evt, localEvt).getValue();
        return new GetValue(input, index);
    }
}

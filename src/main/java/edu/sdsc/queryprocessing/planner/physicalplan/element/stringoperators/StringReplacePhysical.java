package edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.StringReplace;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.stringoperators.ExeStringReplace;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;

public class StringReplacePhysical extends FunctionPhysicalOperator {
    // if the parameter is a variable without value
    private FunctionParameter replacedStrVar;
    private FunctionParameter valueStrVar;
    // if the parameter has value
    private String replacedStr = null;
    private String valueStr = null;

    public StringReplacePhysical(StringReplace p) {
        this.setParameters(p.getParameters());
        this.setInputVar(PlanUtils.physicalVar(p.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(p.getOutputVar()));
        // todo: needs to know if the parameter is a variable or a constant
        List<FunctionParameter> parameters = translateParameters(p.getParameters());
        assert parameters.size()==2;
        this.replacedStrVar = parameters.get(0);
        this.valueStrVar = parameters.get(1);
        if (this.replacedStrVar.isHasValue()) {
            this.replacedStr = (String) this.replacedStrVar.getValue();
        }
        if (this.valueStrVar.isHasValue()) {
            this.valueStr = (String) this.valueStrVar.getValue();
        }
    }

    public FunctionParameter getReplacedStrVar() {
        return replacedStrVar;
    }

    public String getReplacedStr() {
        return replacedStr;
    }

    public String getValueStr() {
        return valueStr;
    }

    public FunctionParameter getValueStrVar() {
        return valueStrVar;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        String replacedStr = this.getReplacedStr();
        String valueStr = this.getValueStr();

        if (replacedStr==null) {
            replacedStr = (String) this.getReplacedStrVar().getValueWithExecutionResult(evt, localEvt);
        }
        if (valueStr==null) {
            valueStr = (String) this.getValueStrVar().getValueWithExecutionResult(evt, localEvt);
        }
        Pair<String, String> input = new Pair<>(replacedStr, valueStr);
        return new ExeStringReplace(input);
    }
}

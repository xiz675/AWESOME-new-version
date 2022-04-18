package edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeString;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.StringJoin;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.stringoperators.ExeStringJoin;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;

public class StringJoinPhysical extends FunctionPhysicalOperator {
    // when these parameters are variables
    private FunctionParameter stringList;
    // true values, assign them if they are constant
    private String joiner;
    private List<String> stringListValue = null;

    public StringJoinPhysical(StringJoin pr) {
        this.setParameters(pr.getParameters());
        this.setInputVar(PlanUtils.physicalVar(pr.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(pr.getOutputVar()));
        // todo: needs to know if the parameter is a variable or a constant
        this.setPipelineCapability(PipelineMode.block);
        List<FunctionParameter> parameters = translateParameters(pr.getParameters());
        assert parameters.size()==2;
        this.joiner = (String) parameters.get(0).getValue();
        this.stringList = parameters.get(1);
        if (this.stringList.isHasValue()) {
            this.stringListValue = (List<String>) this.stringList.getValue();
        }
    }


    public String getJoiner() {
        return joiner;
    }

    public void setJoiner(String joiner) {
        this.joiner = joiner;
    }

    public FunctionParameter getStringListVar() {
        return stringList;
    }

    public List<String> getStringListValue() {
        return stringListValue;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        String joiner = this.getJoiner();
        List<String> toJoinStrings = this.getStringListValue();
        if (joiner==null) {
            joiner = (String) this.getStringListVar().getValueWithExecutionResult(evt, localEvt);
        }
        if (toJoinStrings==null) {
            List tempValue = (List) this.getStringListVar().getValueWithExecutionResult(evt, localEvt);
            toJoinStrings = new ArrayList<>();
            if (tempValue.size() > 0) {
                Object t = tempValue.get(0);
                if (t instanceof AwesomeString) {
                    for (AwesomeString i : (List<AwesomeString>) tempValue) {
                        toJoinStrings.add(i.getValue());
                    }
                }
                else {
                    toJoinStrings = (List<String>) tempValue;
                }
            }
        }
        return new ExeStringJoin(toJoinStrings, joiner);
    }
}

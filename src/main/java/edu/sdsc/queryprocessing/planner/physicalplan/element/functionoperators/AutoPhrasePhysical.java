package edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.AutoPhrase;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.ExecuteAutoPhrase;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AutoPhrasePhysical extends FunctionPhysicalOperator {
    public AutoPhrasePhysical(AutoPhrase auto, Integer var, String inString, String outString){
        JsonArray parameters = auto.getParameters();
        this.setParameters(parameters);
        Set<Integer> inputvar = auto.getInputVar();
        Set<Pair<Integer, String>> input = new HashSet<>();
        for (Integer s : inputvar) {
            Pair<Integer, String> crt;
            if (s.equals(var)) {
                crt = new Pair<>(s, inString);
            }
            else {
                crt = new Pair<>(s, "*");}
            input.add(crt);
        }
        this.setInputVar(input);
        Integer out = auto.getOutputVar().iterator().next();
        this.setOutputVar(Set.of(new Pair<>(out, outString)));
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new ExecuteAutoPhrase(this, config, vt);
    }
}

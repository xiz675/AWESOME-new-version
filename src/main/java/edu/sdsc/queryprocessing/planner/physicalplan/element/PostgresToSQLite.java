package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PostgresToSQLite extends PhysicalOperator {
    public PostgresToSQLite(Set<Pair<Integer, String>> outputVar) {
        this.setInputVar(outputVar);
        Set<Pair<Integer, String>> out = new HashSet<>();
        for (Pair<Integer, String> i : outputVar) {
            out.add(new Pair<>(i.first, "*"));
        }
        this.setOutputVar(out);
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

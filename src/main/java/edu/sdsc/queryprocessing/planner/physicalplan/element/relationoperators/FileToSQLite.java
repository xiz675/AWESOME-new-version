package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileToSQLite extends PhysicalOperator {


    public FileToSQLite(String path, Integer varID) {
        Set<Pair<Integer, String>> in = new HashSet<>();
        in.add(new Pair<>(varID, path));
        Set<Pair<Integer, String>> out = new HashSet<>();
        out.add(new Pair<>(varID, "*"));
        this.setInputVar(in);
        this.setOutputVar(out);
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

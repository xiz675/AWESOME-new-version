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

public class SQLiteToFile extends PhysicalOperator{
    private String sql;
    private String path;
    private String type;

    public SQLiteToFile(String sql,  Integer varID, String path, String type) {
        assert type.equals("txt") || type.equals("csv");
        this.sql = sql;
        this.path = path;
        this.type = type;
        Set<Pair<Integer, String>> out = new HashSet<>();
        out.add(new Pair<>(varID, path));
        Set<Pair<Integer, String>> in = new HashSet<>();
        in.add(new Pair<>(varID, "*"));
        this.setInputVar(in);
        this.setOutputVar(out);
    }

    public String getSql() {
        return sql;
    }

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

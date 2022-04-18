package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecuteSQL extends DBOperator {
    private List<String> tables = new ArrayList<>();

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
    public List<String> getTables() {
        return tables;
    }

}

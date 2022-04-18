package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import javax.json.JsonArray;
import java.util.List;

import static edu.sdsc.utils.JsonUtil.jsonArrayToStringList;

public class ExecuteSolr extends DBOperator{
    private List<String> columns;
    public ExecuteSolr(JsonArray cols) {
        this.columns = jsonArrayToStringList(cols);
    }

    public List<String> getColumns() {
        return columns;
    }
}

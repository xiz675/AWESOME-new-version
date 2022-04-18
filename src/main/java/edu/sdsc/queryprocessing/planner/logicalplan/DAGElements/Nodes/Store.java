package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import javax.json.JsonObject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.sdsc.utils.JsonUtil.jsonArrayToStringList;

public class Store extends Operator {
    private String dbName;
    private String tableName;
    private boolean rowIndex = false;
    private boolean columnIndex = false;
    private boolean index = false;
    private List<String> columnNames;
    private Integer storeVarId;


    public Store(JsonObject detail) {
        this.dbName = detail.getString("DBName");
        this.tableName = detail.getString("TableName");
        this.columnNames = jsonArrayToStringList(detail.getJsonArray("colNames"));
        if (detail.containsKey("index")) {
            this.index = string2Bool(detail.getString("index"));
        }
        if (detail.containsKey("rowIndex")) {
            this.rowIndex = string2Bool(detail.getString("rowIndex"));
        }
        if (detail.containsKey("columnIndex")) {
            this.columnIndex = string2Bool(detail.getString("columnIndex"));
        }
        storeVarId = detail.getInt("varID");
        Set<Integer> var = new HashSet<>();
        var.add(storeVarId);
        this.setInputVar(var);
    }

    public Store(String dbName, String tableName, List<String> columnNames) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public void setColumnIndex(boolean columnIndex) {
        this.columnIndex = columnIndex;
    }

    public void setRowIndex(boolean rowIndex) {
        this.rowIndex = rowIndex;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Integer getStoreVarId() {
        return storeVarId;
    }

    public boolean isIndex() {
        return index;
    }

    public boolean isColumnIndex() {
        return columnIndex;
    }

    public boolean isRowIndex() {
        return rowIndex;
    }

    private boolean string2Bool(String s) {
        return s.equals("True");
    }

}

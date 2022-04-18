package edu.sdsc.variables.logicalvariables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RelationTableEntry extends VariableTableEntry {
    private Map<String, String> schema = new HashMap<>();
    private ArrayList<String> groupByList;
    private ArrayList<String> orderByList;
    public RelationTableEntry(String name, Integer... block){
        super(name, DataTypeEnum.Relation.ordinal(), block);
    }
    public RelationTableEntry(String name,  Map<String, String> schema, Integer... block){
        super(name, DataTypeEnum.Relation.ordinal(), block);
        this.schema = schema;
    }
    @Override
    public Map getPropertyMap(){
        Map propertyMap = new HashMap();
        propertyMap = super.getPropertyMap();
        propertyMap.put("schema", schema);
        return propertyMap;
    }

    public Map<String, String> getSchema() {
        return schema;
    }

    public String getColumnType(String name){
        return schema.get(name);
    }

    public ArrayList<String> getGroupByList() {
        return groupByList;
    }

    public void setGroupByList(ArrayList<String> groupByList) {
        this.groupByList = groupByList;
    }

    public void setOrderByList(ArrayList<String> orderByList) {
        this.orderByList = orderByList;
    }

    public ArrayList<String> getOrderByList() {
        return orderByList;
    }

}
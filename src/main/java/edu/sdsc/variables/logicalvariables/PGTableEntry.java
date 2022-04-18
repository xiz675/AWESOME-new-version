package edu.sdsc.variables.logicalvariables;

import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.Map;

import edu.sdsc.utils.ParserUtil;

import static edu.sdsc.utils.JsonUtil.jsonArrayToMap;

public class PGTableEntry extends VariableTableEntry {
    Map<String, String> nodeProperty;
    Map<String, String> edgeProperty;
    ArrayList<String> nodeLabel;
    ArrayList<String> edgeLabel;

    public PGTableEntry(String name, Integer... block){
        super(name, DataTypeEnum.PropertyGraph.ordinal(), block);
    }
    public PGTableEntry(String name, JsonArray nodeLabel, JsonArray nodeProp,JsonArray edgeLabel,JsonArray edgeProp, Integer... block){
        super(name, DataTypeEnum.PropertyGraph.ordinal(), block);
        this.nodeProperty = jsonArrayToMap(nodeProp, "name", "type");
        this.nodeLabel = ParserUtil.jsonArrayToStringList(nodeLabel);
        this.edgeProperty = jsonArrayToMap(edgeProp, "name", "type");
        this.edgeLabel = ParserUtil.jsonArrayToStringList(edgeLabel);
    }

    public void setProperty(String type, String name, String... objType){
        if (type.equals("nodeProp")){
            nodeProperty.put(name, objType[0]);
        }
        else if (type.equals("nodeLabel")){
            nodeLabel.add(name);
        }
        else if (type.equals("edgeProp")){
            edgeProperty.put(name, objType[0]);
        }
        else{edgeLabel.add(name);}
    }

    public boolean isExisted(String type, String name){
        if (type.equals("nodeProp")){
            return nodeProperty.containsKey(name);
        }
        else if (type.equals("nodeLabel")){
            return nodeLabel.contains(name);
        }
        else if (type.equals("edgeProp")){
            return edgeProperty.containsKey(name);
        }
        else{return edgeLabel.contains(name);}
    }

    public String getProperty(String name, String type) throws RuntimeException{
        if (type.equals("node")){
            return getString(name, nodeProperty, nodeLabel);
        }
        else {
            return getString(name, edgeProperty, edgeLabel);
        }
    }

    private String getString(String name, Map<String, String> property, ArrayList<String> label) {
        if (property.containsKey(name)) {
            return property.get(name);
        }
        else if(label.contains(name)){
            return "label";
        }
        else {
            throw new RuntimeException();
        }
    }

}

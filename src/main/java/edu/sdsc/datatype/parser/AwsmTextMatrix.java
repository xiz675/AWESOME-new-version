package edu.sdsc.datatype.parser;



import edu.sdsc.variables.logicalvariables.TextMatrixTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class AwsmTextMatrix extends AwsmDataType{
    public AwsmTextMatrix(String name){
        this.name = name;
    }



    @Override
    public VariableTableEntry toTableEntry(Integer... block) {
        return new TextMatrixTableEntry(name, block);
    }

    @Override
    public JsonObject toJsonObject(Integer vID, Integer... block) {
        JsonObjectBuilder tempJB = Json.createObjectBuilder().add("varName", name).add("varID", vID).add("varType", "TextMatrix");
        return tempJB.build();
    }
}

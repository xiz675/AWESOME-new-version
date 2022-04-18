package edu.sdsc.datatype.parser;

import edu.sdsc.variables.logicalvariables.MatrixTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class AwsmMatrix extends AwsmDataType {
    public AwsmMatrix(String name){
        this.name = name;
    }



    @Override
    public VariableTableEntry toTableEntry(Integer... block) {
        return new MatrixTableEntry(name, block);
    }

    @Override
    public JsonObject toJsonObject(Integer vID, Integer... block) {
        JsonObjectBuilder tempJB = Json.createObjectBuilder().add("varName", name).add("varID", vID).add("varType", "Matrix");
        return tempJB.build();
    }
}

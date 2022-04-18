package edu.sdsc.datatype.parser;

import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.LiteralTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class AwsmLiteral extends AwsmDataType {
    private String type;
    public AwsmLiteral(String name, String type){
        this.name = name;
        this.type = type;
    }
    @Override
    public VariableTableEntry toTableEntry(Integer... block) {
        return new LiteralTableEntry(name, DataTypeEnum.valueOf(type).ordinal(), block);
    }

    @Override
    public JsonObject toJsonObject(Integer vID, Integer... block) {
        JsonObjectBuilder tempJB = Json.createObjectBuilder().add("varName", name).add("varID", vID).add("varType", type);
        return tempJB.build();
    }
}

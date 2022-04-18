package edu.sdsc.datatype.parser;



import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.ListTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class AwsmList extends AwsmDataType {
    public String elmtType = null;

    @Override
    public VariableTableEntry toTableEntry(Integer... block) {
        ListTableEntry rslt = new ListTableEntry(name, block);
        if (elmtType!=null){
            rslt.setElementType(DataTypeEnum.valueOf(elmtType).ordinal());
        }
        return rslt;
    }

    @Override
    public JsonObject toJsonObject(Integer vID, Integer... block) {
        JsonObjectBuilder tempJB = Json.createObjectBuilder().add("varName", name).add("varID", vID).add("varType", "List");
        if (block.length==1){
            tempJB.add("blockID", block[0]);
        }
        if (elmtType!=null){
            tempJB.add("elementType", elmtType);
        }
        return tempJB.build();
    }
}

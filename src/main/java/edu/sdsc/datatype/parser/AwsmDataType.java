package edu.sdsc.datatype.parser;


import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.JsonObject;

public abstract class AwsmDataType {
    public String name;
    public abstract VariableTableEntry toTableEntry(Integer... block);
    public abstract JsonObject toJsonObject(Integer vID, Integer... block);

}

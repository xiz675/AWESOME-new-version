package edu.sdsc.variables.logicalvariables;

public class BoolTableEntry extends VariableTableEntry {
    public BoolTableEntry(String name, Integer... blockID){
        super(name, DataTypeEnum.Boolean.ordinal(), blockID);
    }

    public BoolTableEntry(String name, Boolean value, Integer... blockID){
        super(name, DataTypeEnum.List.ordinal(), blockID);
        setValue(value);
    }
}

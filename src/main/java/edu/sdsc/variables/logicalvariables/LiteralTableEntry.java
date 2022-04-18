package edu.sdsc.variables.logicalvariables;

public class LiteralTableEntry extends VariableTableEntry {
    //private Object value = null;
    public LiteralTableEntry(String name, Integer type, Integer... blockID){
        super(name, type, blockID);
    }
    public LiteralTableEntry(String name, Integer type, Object value, Integer... blockID){
        super(name, type, blockID);
        setValue(value);
    }
}

package edu.sdsc.variables.logicalvariables;

public class UndecidedTableEntry extends VariableTableEntry {
    public UndecidedTableEntry(String name, Integer... block){
        super(name, DataTypeEnum.Undecided.ordinal(), block);
    }

}

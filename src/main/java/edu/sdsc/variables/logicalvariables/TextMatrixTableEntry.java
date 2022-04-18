package edu.sdsc.variables.logicalvariables;

public class TextMatrixTableEntry extends VariableTableEntry{
    private boolean singleColumn = false;
    private boolean singleRow = false;
    public TextMatrixTableEntry(String name, Integer... block){
        super(name, DataTypeEnum.TextMatrix.ordinal(), block);
    }
    public TextMatrixTableEntry(String name, String mode, Integer... block) {
        super(name, DataTypeEnum.TextMatrix.ordinal(), block);
        if (mode.equals("TextMatrixColumn")) {
            singleColumn = true;
        }
        else if (mode.equals("TextMatrixRow")) {
            singleRow = true;
        }
    }

    public boolean isSingleColumn() {
        return singleColumn;
    }

    public boolean isSingleRow() {
        return singleRow;
    }

    @Override
    public Integer getType() {
        if (isSingleColumn()) {
            return DataTypeEnum.valueOf("MatrixColumn").ordinal();
        }
        else if (isSingleRow()) {
            return DataTypeEnum.valueOf("MatrixRow").ordinal();
        }
         return super.getType();
    }
}

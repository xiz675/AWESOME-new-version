package edu.sdsc.variables.logicalvariables;


import edu.sdsc.utils.Pair;
import org.ejml.data.DMatrixRMaj;

import java.util.HashMap;
import java.util.Map;


public class MatrixTableEntry extends VariableTableEntry {
    private Integer row =0;
    private  Integer col =0;
    public MatrixTableEntry(String name,  Integer... block){
        super(name, DataTypeEnum.Matrix.ordinal(), block);
    }
    public MatrixTableEntry(String name, Integer row, Integer col, Integer... block){
        super(name, DataTypeEnum.Matrix.ordinal(), block);
        this.row = row;
        this.col = col;

    }
    public MatrixTableEntry(String name, double[][] value, Integer... block){
        super(name, DataTypeEnum.Matrix.ordinal(), block);
        if (value!=null) {
            this.row = value.length;
            this.col = value[0].length;
        }
        DMatrixRMaj x = new DMatrixRMaj(value);
        setValue(x);
    }

    public Integer getRow() {
        return row;
    }

    public Integer getCol() {
        return col;
    }

    public double getElement(Integer row, Integer col) {
        DMatrixRMaj value = (DMatrixRMaj) this.getValue();
        return value.get(row, col);
    }

    public Pair getDim(){
        int row = getRow();
        int col = getCol();
        return new Pair(row, col);
    }

    @Override
    public Map getPropertyMap(){
        Map propertyMap = new HashMap();
        propertyMap = super.getPropertyMap();
        return propertyMap;
    }


}

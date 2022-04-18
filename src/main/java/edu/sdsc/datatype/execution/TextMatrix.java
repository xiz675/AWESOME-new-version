package edu.sdsc.datatype.execution;

import org.ejml.data.DMatrixRMaj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextMatrix {
    private List rowMapping = new ArrayList();
    private List colMapping = new ArrayList();
    private DMatrixRMaj value;

    public TextMatrix() {

    }

    public TextMatrix(List rowMappinp, List colMapping) {
        this.rowMapping = rowMappinp;
        this.colMapping = colMapping;
    }


    public TextMatrix(List rowMappinp, List colMapping, DMatrixRMaj value) {
        this.rowMapping = rowMappinp;
        this.colMapping = colMapping;
        this.value = value;
    }

    public void setColMapping(List colMapping) {
        this.colMapping = colMapping;
    }

    public void setRowMapping(List rowMappinp) {
        this.rowMapping = rowMappinp;
    }

    public void setValue(DMatrixRMaj value) {
        if (rowMapping != null) {
            assert  rowMapping.size() == value.numRows;
        }
        if (colMapping != null) {
            assert  colMapping.size() == value.numCols;
        }
        this.value = value;
    }

    public DMatrixRMaj getValue() {
        return value;
    }

    public List getColMapping() {
        return colMapping;
    }

    public List getRowMapping() {
        return rowMapping;
    }

    public Integer getRowNum() {
        return this.value.numRows;
    }

    public Integer getColNum() {
        return this.value.numCols;
    }
}

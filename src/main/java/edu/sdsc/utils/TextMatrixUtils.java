package edu.sdsc.utils;

import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import org.ejml.data.DMatrixRMaj;

import java.util.ArrayList;
import java.util.List;

public class TextMatrixUtils {
    public static TextMatrix createTextMatrixFromColumns(List rows, List cols, List<double[]> values) {
        double[][] value = new double[rows.size()][cols.size()];
        for (double[] crtCol : values) {
            for (int row=0; row < rows.size(); row++) {
                value[row] = crtCol;
            }
        }
        return new TextMatrix(rows, cols, new DMatrixRMaj(value));
    }

    public static TextMatrix createTextMatrixFromRows(List rows, List cols, List<double[]> values) {
        if (values.size() == 0) {return new TextMatrix(rows, cols, null);}
        double[][] value = new double[rows.size()][cols.size()];
        int row = 0;
        for (double[] crtRow : values) {
            value[row] = crtRow;
            row += 1;
        }
        return new TextMatrix(rows, cols, new DMatrixRMaj(value));
    }

    public static TextMatrix getRow(TextMatrix value, int rowIndex) {
        List<Object> rowName = new ArrayList<>();
        rowName.add(value.getRowMapping().get(rowIndex));
        List colNames = value.getColMapping();
        DMatrixRMaj matrixVal = value.getValue();
        int colNum = matrixVal.numCols;
        double[][] crtRow = new double[1][colNum];
        for (int i=rowIndex*colNum; i<(rowIndex+1)*colNum; i++) {
            crtRow[0][i-rowIndex*colNum] = matrixVal.get(i);
        }
        return new TextMatrix(rowName, colNames, new DMatrixRMaj(crtRow));
    }

    public static TextMatrix getCol(TextMatrix value, int colIndex) {
        List<Object> colName = new ArrayList<>();
        colName.add(value.getColMapping().get(colIndex));
        List rowNames = value.getRowMapping();
        DMatrixRMaj matrixVal = value.getValue();
        int colNum = matrixVal.numCols;
        int rowNum = matrixVal.numRows;
        double[][] crtCol = new double[rowNum][1];
        for (int i=0; i<rowNum; i++) {
            crtCol[i][0] = matrixVal.get(colIndex + i*colNum);
        }
        return new TextMatrix(rowNames, colName, new DMatrixRMaj(crtCol));
    }


    public static void main(String[] args) {
        TextMatrix x = new TextMatrix(new ArrayList(), new ArrayList(), null);
//        System.out.println(x.getValue());
        MaterializedList s = new MaterializedList(x.getRowMapping());
    }


}

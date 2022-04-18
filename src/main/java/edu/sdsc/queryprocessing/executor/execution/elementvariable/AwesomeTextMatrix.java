package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.TextMatrix;

public class AwesomeTextMatrix extends ExecutionTableEntryMaterialized<TextMatrix>{
    public AwesomeTextMatrix(TextMatrix textMatrix) {
        this.setValue(textMatrix);
    }




}

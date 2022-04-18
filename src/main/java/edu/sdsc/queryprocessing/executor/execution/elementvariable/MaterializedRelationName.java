package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.utils.Pair;

public class MaterializedRelationName extends ExecutionTableEntryMaterialized<Pair<String, String>> {
    public MaterializedRelationName() {

    }
    public MaterializedRelationName(Pair<String, String> relation) {
        this.setValue(relation);
    }

}
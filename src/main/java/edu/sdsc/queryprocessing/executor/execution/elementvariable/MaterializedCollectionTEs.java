package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import java.util.ArrayList;
import java.util.List;

public class MaterializedCollectionTEs extends ExecutionTableEntryMaterialized<List<ExecutionTableEntryMaterialized>> {
    public MaterializedCollectionTEs() {
        this.setValue(new ArrayList<>());
    }

    public MaterializedCollectionTEs(List<ExecutionTableEntryMaterialized> value) {
        this.setValue(value);
    }

    public void addToCollection(ExecutionTableEntryMaterialized singleValue) {
        List<ExecutionTableEntryMaterialized> value = this.getValue();
        value.add(singleValue);
        this.setValue(value);
    }
}

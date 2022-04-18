package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import java.util.List;

// todo:  for the collection which is used as a normal list later (no HLOs applied on it), transform it to a normal List type

public class MaterializedHLCollection extends ExecutionTableEntryMaterialized<List<ExecutionTableEntry>> {
    public MaterializedHLCollection(List<ExecutionTableEntry> value) {
        this.setValue(value);
    }

    public void addValue(ExecutionTableEntry x) {
        this.getValue().add(x);
    }

}

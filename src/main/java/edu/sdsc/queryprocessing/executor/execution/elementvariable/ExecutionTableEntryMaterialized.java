package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import java.util.ArrayList;
import java.util.List;

public class ExecutionTableEntryMaterialized<T> implements ExecutionTableEntry<T> {
    private T value = null;
    private List<T> partitionedValue = new ArrayList<>();
    private boolean isPartitioned = false;

    public ExecutionTableEntryMaterialized() {}

    public ExecutionTableEntryMaterialized(T value) {
        this.value = value;
    }

    public ExecutionTableEntryMaterialized(List<T> value, boolean flag) {
        assert flag;
        setPartitionedValue(value);
    }
//
    public T getValue() {
        return this.value;
    }

    public List<T> getPartitionedValue() {
        return this.partitionedValue;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public void setValue(T value) {
        this.value = value;
    }
    public void setPartitionedValue(List<T> value) {this.partitionedValue = value; this.isPartitioned = true;}
    public String toSQL() {
        return null;
    }
    public String toCypher() {return null;}
    public String toSolr() {return null;}
}

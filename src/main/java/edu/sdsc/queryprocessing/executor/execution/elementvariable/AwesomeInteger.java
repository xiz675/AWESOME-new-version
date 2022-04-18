package edu.sdsc.queryprocessing.executor.execution.elementvariable;

public class AwesomeInteger extends ExecutionTableEntryMaterialized<Integer> {

    public AwesomeInteger(Integer i) {
        this.setValue(i);
    }

    @Override
    public String toSQL() {
        return this.getValue().toString();
    }

    @Override
    public String toCypher() {
        return this.getValue().toString();
    }
}

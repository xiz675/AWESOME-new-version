package edu.sdsc.queryprocessing.executor.execution.elementvariable;

public class AwesomeDouble extends ExecutionTableEntryMaterialized<Double> {
private Double value = this.getValue();

    public AwesomeDouble(Double x) {
        this.setValue(x);
    }

    @Override
    public String toSQL() {
        return value.toString();
    }
}

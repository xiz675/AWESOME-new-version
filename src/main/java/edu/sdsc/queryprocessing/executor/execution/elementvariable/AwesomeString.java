package edu.sdsc.queryprocessing.executor.execution.elementvariable;

public class AwesomeString extends ExecutionTableEntryMaterialized<String> {

    public AwesomeString(String value) {
        this.setValue(value);
    }

    @Override
    public String toSQL() {
        return "'"+ this.getValue() +"'";
    }

    @Override
    public String toSolr() {
        return this.getValue();
    }

    @Override
    public String toCypher() {
        return this.getValue();
    }
}

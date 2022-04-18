package edu.sdsc.queryprocessing.executor.execution.elementvariable;

// todo: Let the two abstract class implement it and change the ExecuteVariableTable and the StreamInput/output interface
public interface ExecutionTableEntry<T> {
    T getValue();

    void setValue(T value);

    String toSQL();
    String toCypher();

}

package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import org.reactivestreams.Publisher;

import java.util.stream.Stream;

public abstract class ExecutionTableEntryStream<T> implements ExecutionTableEntry<Stream<T>> {
    private Stream<T> values = null;

    @Override
    public Stream<T> getValue() {
        return this.values;
    }

    @Override
    public void setValue(Stream<T> value) {
        this.values = value;
    }

    public String toSQL() {
        return null;
    }
    public String toCypher() {return null;}

}

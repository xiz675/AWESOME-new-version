package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import org.reactivestreams.Publisher;

import java.util.stream.Stream;


public class StreamHLCollection extends ExecutionTableEntryStream<ExecutionTableEntry> {
    public StreamHLCollection(Stream<ExecutionTableEntry> value) {
        this.setValue(value);
    }
}

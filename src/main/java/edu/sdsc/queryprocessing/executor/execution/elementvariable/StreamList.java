package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import org.reactivestreams.Publisher;

import java.util.stream.Stream;

public class StreamList<T> extends ExecutionTableEntryStream<T> {
    public StreamList (Stream<T> listStream) {this.setValue(listStream);}

}

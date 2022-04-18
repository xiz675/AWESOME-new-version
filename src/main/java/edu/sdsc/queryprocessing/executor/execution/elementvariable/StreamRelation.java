package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.AwesomeRecord;
import org.reactivestreams.Publisher;

import java.util.stream.Stream;

public class StreamRelation extends ExecutionTableEntryStream<AwesomeRecord> {
    public StreamRelation(Stream<AwesomeRecord> records) {this.setValue(records);}
}
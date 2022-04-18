package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.GraphElement;
import org.reactivestreams.Publisher;

import java.util.stream.Stream;

public class StreamGraphData extends ExecutionTableEntryStream<GraphElement>{
    public StreamGraphData(Stream<GraphElement> graphData) {this.setValue(graphData);}
}

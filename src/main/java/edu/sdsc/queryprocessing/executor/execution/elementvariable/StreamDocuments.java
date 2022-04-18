package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.Document;
import org.reactivestreams.Publisher;

import java.util.stream.Stream;

public class StreamDocuments extends ExecutionTableEntryStream<Document> {

    public StreamDocuments(Stream<Document> docs) {this.setValue(docs);}

}

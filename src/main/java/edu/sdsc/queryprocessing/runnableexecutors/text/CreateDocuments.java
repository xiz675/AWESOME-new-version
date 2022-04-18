package edu.sdsc.queryprocessing.runnableexecutors.text;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedDocuments;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// for document, use docID null as EOF, for AwesomeRecord, use a boolean

public class CreateDocuments extends AwesomePipelineRunnable<AwesomeRecord, Document> {
    private String docIDColName;
    private String textColName;

    // for parallel
    public CreateDocuments(PipelineMode m, String docIDCol, String textCol) {
        super(m);
        this.docIDColName = docIDCol;
        this.textColName = textCol;
    }

    // stream in constructor
    public CreateDocuments(Stream<AwesomeRecord> data, String docIDColName, String textColName) {
        super(data);
        this.docIDColName = docIDColName;
        this.textColName = textColName;
    }

    // stream out constructor
    public CreateDocuments(List<AwesomeRecord> data, String docIDColName, String textColName) {
        super(data);
        this.docIDColName = docIDColName;
        this.textColName = textColName;
    }

    // pipeline constructor:
    public CreateDocuments(Stream<AwesomeRecord> data, String docIDColName, String textColName, boolean streamOut) {
        super(data, streamOut);
        assert streamOut;
        this.docIDColName = docIDColName;
        this.textColName = textColName;
    }

    // blocking constructor:
    public CreateDocuments(List<AwesomeRecord> input, String docIDColName, String textColName, boolean materializeOut) {
        super(input, materializeOut);
        assert materializeOut;
        this.docIDColName = docIDColName;
        this.textColName = textColName;
    }

    public void executeStreamInput() {
        setMaterializedOutput(getStreamInput().map(this::executor).collect(Collectors.toList()));
    }

    public void executeStreamOutput() {
        setStreamOutput(getMaterializedInput().stream().map(this::executor));
    }

    public void executePipeline() {
        setStreamOutput(getStreamInput().map(this::executor));
    }

    public void executeBlocking() {
        List<AwesomeRecord> materializedInput = getMaterializedInput();
        List<Document> materializedResult = new ArrayList<>();
        for (AwesomeRecord t:materializedInput) {
            materializedResult.add(executor(t));
        }
        setMaterializedOutput(materializedResult);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamDocuments(this.getStreamResult());
        }
        else {
            return new MaterializedDocuments(this.getMaterializedOutput());
        }
    }

    private Document executor(AwesomeRecord r) {
        return new Document(r.getColumn(docIDColName), (String) r.getColumn(textColName));
    }

}

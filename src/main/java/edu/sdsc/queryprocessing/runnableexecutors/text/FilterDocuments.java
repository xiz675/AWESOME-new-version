package edu.sdsc.queryprocessing.runnableexecutors.text;

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

// todo: only need block execution for now
public class FilterDocuments extends AwesomePipelineRunnable<Document, Document> {
    private List<String> stopwords = new ArrayList<>();
    final private Integer unitSize = 500;

    public FilterDocuments(PipelineMode mode, List<String> stopwords) {
        super(mode);
        this.stopwords = stopwords;
    }

    // stream in constructor
    public FilterDocuments(Stream<Document> data, List<String> stopwords) {
        super(data);
        this.stopwords = stopwords;
    }

    // stream out constructor
    public FilterDocuments(List<Document> data, List<String> stopwords) {
        super(data);
        this.stopwords = stopwords;
    }

    // pipeline constructor:
    public FilterDocuments(Stream<Document> data, List<String> stopwords, boolean streamOut) {
        super(data, true);
        this.stopwords = stopwords;
        assert streamOut;
    }

    // blocking constructor:
    public FilterDocuments(List<Document> input, List<String> stopwords, boolean materializeOut) {
        super(input, true);
        assert materializeOut;
        this.stopwords = stopwords;
    }

    @Override
    public void executeStreamInput() {
        Stream<Document> input = getStreamInput();
        setMaterializedOutput(input.map(this::execute).collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        setStreamOutput(getMaterializedInput().stream().map(this::execute));
    }

    @Override
    public void executePipeline() {
        Stream<Document> input = getStreamInput();
        setStreamOutput(input.map(this::execute));
    }

    @Override
    public void executeBlocking() {
        List<Document> materializedInput = getMaterializedInput();
        List<Document> materializedResult = new ArrayList<>();
        for (Document s:materializedInput) {
            materializedResult.add(execute(s));
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

    private Document execute(Document s) {
        s.setTokens(s.tokens.stream().filter(x -> !stopwords.contains(x)).collect(Collectors.toList()));
        return s;
    }
}

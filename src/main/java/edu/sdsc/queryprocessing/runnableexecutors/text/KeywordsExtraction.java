package edu.sdsc.queryprocessing.runnableexecutors.text;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeywordsExtraction extends AwesomePipelineRunnable<Document, List<String>> {
    private List<String> keywords = new ArrayList<>();
    final private Integer unitSize = 500;

    // this is for creating multiple instance for parallel execution
    public KeywordsExtraction(PipelineMode mode) {
        super(mode);
    }

    public KeywordsExtraction(PipelineMode mode, List<String> keywords) {
        super(mode);
        this.keywords = keywords;
    }

    // stream in constructor
    public KeywordsExtraction(Stream<Document> data, List<String> keywords) {
        super(data);
        this.keywords = keywords;
    }

    // stream out constructor
    public KeywordsExtraction(List<Document> data, List<String> keywords) {
        super(data);
        this.keywords = keywords;
    }

    // pipeline constructor:
    public KeywordsExtraction(Stream<Document> data, List<String> keywords, boolean streamOut) {
        super(data, true);
        this.keywords = keywords;
        assert streamOut;
    }

    // blocking constructor:
    public KeywordsExtraction(List<Document> input, List<String> keywords, boolean materializeOut) {
        super(input, true);
        this.keywords = keywords;
        assert materializeOut;
    }


    @Override
    public void executeStreamInput() {
        List<List<String>> output = getStreamInput().map(this::execute).collect(Collectors.toList());
        setMaterializedOutput(output);
    }

    @Override
    public void executeStreamOutput() {
        setStreamOutput(getMaterializedInput().stream().map(this::execute));
    }

    @Override
    public void executePipeline() {
        setStreamOutput(getStreamInput().map(this::execute));
    }

    @Override
    public void executeBlocking() {
//        long start = System.currentTimeMillis();
        List<Document> materializedInput = getMaterializedInput();
        List<List<String>> materializedResult = new ArrayList<>();
        for (Document s : materializedInput) {
            List<String> temp = execute(s);
            materializedResult.add(temp);
            // materializedResult.add(s.tokens.stream().filter(x -> keywords.contains(x)).collect(Collectors.toList()));
        }
        setMaterializedOutput(materializedResult);
//        System.out.println(String.format("keywords extraction costs: %d ms",  System.currentTimeMillis() - start));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    private List<String> execute(Document doc) {
        List<String> temp = new ArrayList<>();
        for (String i : doc.tokens) {
            if (keywords.contains(i)) {
                temp.add(i);
            }
        }
        return temp;
    }

}

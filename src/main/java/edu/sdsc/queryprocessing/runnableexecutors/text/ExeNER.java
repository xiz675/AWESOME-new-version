package edu.sdsc.queryprocessing.runnableexecutors.text;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.Triple;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExeNER extends AwesomePipelineRunnable<AwesomeRecord, AwesomeRecord> {
    private AbstractSequenceClassifier<CoreLabel> model;
    private String colName;
    private String path;

    // for parallel, the input will be set in the outside function
    public ExeNER(PipelineMode mode, String modelPath, String colName) {
        super(mode);
        this.colName = colName;
        this.path = modelPath;
    }

    // stream in
    public ExeNER(Stream<AwesomeRecord> input, String modelPath, String colName) {
        this(PipelineMode.streaminput, modelPath, colName);
        this.setStreamInput(input);
    }

    // pipeline
    public ExeNER(Stream<AwesomeRecord> input, String modelPath, String colName, boolean pipeline) {
        this(PipelineMode.pipeline, modelPath, colName);
        assert pipeline;
        this.setStreamInput(input);
    }

    // stream out
    public ExeNER(List<AwesomeRecord> input, String modelPath, String colName) {
        this(PipelineMode.streamoutput, modelPath, colName);
        this.setMaterializedInput(input);
    }

    // materialize
    public ExeNER(List<AwesomeRecord> input, String modelPath, String colName, boolean materialize) {
        this(PipelineMode.block, modelPath, colName);
        assert materialize;
        this.setMaterializedInput(input);
    }


    @Override
    public void executeStreamInput() {
        long start = System.currentTimeMillis();
        try{
            model = CRFClassifier.getClassifier(path);
        } catch (IOException|ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("load ner model costs: " + (System.currentTimeMillis() - start));
        Stream<AwesomeRecord> input = getStreamInput();
        setMaterializedOutput(input.map(i -> (String) i.getColumn(colName)).map(this::predict).flatMap(Collection::stream).collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        long start = System.currentTimeMillis();
        try{
            model = CRFClassifier.getClassifier(path);
        } catch (IOException|ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("load ner model costs: " + (System.currentTimeMillis() - start));
        List<AwesomeRecord> input = getMaterializedInput();
        setStreamOutput(input.stream().map(i -> (String) i.getColumn(colName)).map(this::predict).flatMap(Collection::stream));
    }

    @Override
    public void executePipeline() {
        long start = System.currentTimeMillis();
        try{
            model = CRFClassifier.getClassifier(path);
        } catch (IOException|ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("load ner model costs: " + (System.currentTimeMillis() - start));
        Stream<AwesomeRecord> input = getStreamInput();
        setStreamOutput(input.map(i -> (String) i.getColumn(colName)).map(this::predict).flatMap(Collection::stream));
    }

    @Override
    public void executeBlocking() {
        long start = System.currentTimeMillis();
        try{
            model = CRFClassifier.getClassifier(path);
        } catch (IOException|ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("load ner model costs: " + (System.currentTimeMillis() - start));
        List<AwesomeRecord> input = getMaterializedInput();
        List<String> corpus = new ArrayList<>();
        for (AwesomeRecord r : input) {
            corpus.add((String) r.getColumn(colName));
        }
        List<AwesomeRecord> ner = new ArrayList<>();
        for (String sent : corpus) {
            ner.addAll(predict(sent));
        }
        setMaterializedOutput(ner);
        System.out.println("find ner:" + ner.size());
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamRelation(this.getStreamResult());
        }
        else {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
    }


    private List<AwesomeRecord> predict(String sentence) {
        List<AwesomeRecord> result = new ArrayList<>();
        String[] cols = {"type", "name"};
        for (Triple triple : model.classifyToCharacterOffsets(sentence)) {
            Map<String, Object> re = new HashMap<>();
            re.put(cols[0], (String) triple.first());
            int offset0 = (Integer) triple.second();
            int offset1 = (Integer) triple.third();
            String name = sentence.substring(offset0, offset1);
            re.put(cols[1], name);
            result.add(new AwesomeRecord(re));
        }
        return result;
    }

}

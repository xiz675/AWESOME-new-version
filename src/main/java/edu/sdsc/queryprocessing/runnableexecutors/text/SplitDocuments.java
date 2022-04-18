package edu.sdsc.queryprocessing.runnableexecutors.text;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedDocuments;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SplitDocuments extends AwesomePipelineRunnable<Document, Document> {
    private String splitter;
    final private Integer unitSize = 500;
    private StanfordCoreNLP pipeline;

    public SplitDocuments(PipelineMode mode, String splitter) {
        super(mode);
        this.splitter = splitter;
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        // StanfordCoreNLP loads a lot of models, so you probably
        // only want to do this once per execution
        this.pipeline = new StanfordCoreNLP(props);
    }

    // stream in constructor
    public SplitDocuments(Stream<Document> data, String splitter) {
        super(data);
        this.splitter = splitter;
//        Properties props;
//        props = new Properties();
//        props.put("annotators", "tokenize, ssplit, pos, lemma");
//        // StanfordCoreNLP loads a lot of models, so you probably
//        // only want to do this once per execution
//        this.pipeline = new StanfordCoreNLP(props);
    }

    // stream out constructor
    public SplitDocuments(List<Document> data, String splitter) {
        super(data);
        this.splitter = splitter;
//        Properties props;
//        props = new Properties();
//        props.put("annotators", "tokenize, ssplit, pos, lemma");
//        // StanfordCoreNLP loads a lot of models, so you probably
//        // only want to do this once per execution
//        this.pipeline = new StanfordCoreNLP(props);
    }

    // pipeline constructor:
    public SplitDocuments(Stream<Document> data, String splitter, boolean streamOut) {
        super(data, true);
        this.splitter = splitter;
        assert streamOut;
//        Properties props;
//        props = new Properties();
//        props.put("annotators", "tokenize, ssplit, pos, lemma");
//        // StanfordCoreNLP loads a lot of models, so you probably
//        // only want to do this once per execution
//        this.pipeline = new StanfordCoreNLP(props);
    }

    // blocking constructor:
    public SplitDocuments(List<Document> input, String splitter, boolean materializeOut) {
        super(input, materializeOut);
        this.splitter = splitter;
        assert materializeOut;
//        Properties props;
//        props = new Properties();
//        props.put("annotators", "tokenize, ssplit, pos, lemma");
//        this.pipeline = new StanfordCoreNLP(props);
    }

    @Override
    public void executeStreamInput() {
        // StanfordCoreNLP loads a lot of models, so you probably
        // only want to do this once per execution
        setMaterializedOutput(getStreamInput().map(this::execute).collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        List<Document> materializedInput = getMaterializedInput();
        setStreamOutput(materializedInput.stream().map(this::execute));
    }

    @Override
    public void executePipeline() {
        setStreamOutput(getStreamInput().map(this::execute));
    }


    @Override
    public void executeBlocking() {
        List<Document> materializedInput = getMaterializedInput();
        List<Document> materializedResult = new ArrayList<>();
        for (Document t : materializedInput) {
            materializedResult.add(execute(t));
        }
        setMaterializedOutput(materializedResult);
        // System.out.println(String.format("split documents costs: %d ms",   System.currentTimeMillis() - start));
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

    // split, lowercase, stem, lemma
    private List<String> getTokens(String text) {
        List<String> lemmas = new LinkedList<String>();

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // run all Annotators on this text
        this.pipeline.annotate(document);

        // Iterate over all of the sentences found
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            // Iterate over all tokens in a sentence
            for (CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // Retrieve and add the lemma for each word into the list of lemmas
                lemmas.add(token.get(CoreAnnotations.LemmaAnnotation.class));
            }
        }

        return lemmas;
    }

    private Document execute(Document t) {
        Document temp = new Document(t);
//        temp.setTokens(getTokens(t.text));
        temp.setTokens(Arrays.asList(t.text.split(" ")));
        return temp;
    }

    public static void main(String[] args) {

    }
}

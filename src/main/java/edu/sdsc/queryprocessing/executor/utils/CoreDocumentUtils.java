package edu.sdsc.queryprocessing.executor.utils;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

public class CoreDocumentUtils {
    private static final StanfordCoreNLP tokenizePipeline;
    private static final List<String> stopwords;

    static {
        // init coreNLP pipeline
        // use tokenize function only, the full list of annotators is at:
        // https://stanfordnlp.github.io/CoreNLP/annotators.html
        Properties properties = new Properties();
        properties.setProperty("annotators", "tokenize");
        tokenizePipeline = new StanfordCoreNLP(properties);

        // load stopwords
        stopwords = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("stopwords.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                stopwords.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<Integer, CoreDocument> loadRawTextFromDB(Connection conn, String sql) {
        // load text data from database
        // check if primary key is available (can be get from metadata)
        // if not, assign a unique id to each text
        // the read raw texts are in Map<Integer, String>
        Map<Integer, String> rawTextMap = new HashMap<>();

        Map<Integer, CoreDocument> documentMap = new HashMap<>();
        for (int id : rawTextMap.keySet()) {
            CoreDocument document = new CoreDocument(rawTextMap.get(id));
            tokenizePipeline.annotate(document);
            documentMap.put(id, document);
        }

        throw new UnsupportedOperationException("Not yet finished");
    }

    // to use matrix representation, please refer to a library called "ejml", which provides extensive linear algebra
    // support for java and is used by CoreNLP
    // ejml supports several different matrix decomposition algorithms like svd and qr (but no nmf)
    // you can easily implement an nmf using the api provided by ejml


    public static void main(String[] args) {

    }
}

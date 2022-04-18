package edu.sdsc.datatype.execution;

import java.util.List;
import java.util.Map;

public class Document {
    public Object docID;
    public String text;
    public List<String> tokens = null;
    public boolean docEOF = false;

    public Document() {
        this.docEOF = true;
    }

    public Document(Object docID, String text) {
        this.text = text;
        this.docID = docID;
    }

    public Document(Document d) {
        this.docID = d.docID;
//        this.text = d.text;
    }

    public void setTokens(List<String> tokens) {
        this.tokens = tokens;
    }

    public boolean isDocEOF() {
        return docEOF;
    }
}

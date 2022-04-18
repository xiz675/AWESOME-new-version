package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

public class CreateDocumentsMeta extends ExecutorMetaData {
    public String docID;
    public String textID;



    public CreateDocumentsMeta(String docID, String textID) {
        super(ExecutorEnum.CreateDocuments);
        this.docID = docID;
        this.textID = textID;
    }


    public CreateDocumentsMeta(String docID, String textID, PipelineMode mode) {
        super(ExecutorEnum.CreateDocuments, PipelineMode.pipeline, mode);
        this.docID = docID;
        this.textID = textID;
    }

    public String getDocID() {
        return docID;
    }

    public String getTextID() {
        return textID;
    }
}

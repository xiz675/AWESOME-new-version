package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

public class SplitDocumentsMeta extends ExecutorMetaData {
    String splitter;

    public SplitDocumentsMeta(String splitter) {
        super(ExecutorEnum.SplitDocuments);
        this.splitter = splitter;
    }


    public SplitDocumentsMeta(String splitter, PipelineMode mode) {
        super(ExecutorEnum.SplitDocuments, PipelineMode.pipeline, mode);
        this.splitter = splitter;
    }

    public String getSplitter() {
        return splitter;
    }
}

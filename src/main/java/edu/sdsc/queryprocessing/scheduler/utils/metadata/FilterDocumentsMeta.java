package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

import java.util.List;

public class FilterDocumentsMeta extends ExecutorMetaData {
    private List<String> stopwords;

    public FilterDocumentsMeta() {
        super(ExecutorEnum.FilterDocuments);
    }

    public FilterDocumentsMeta(PipelineMode cap, PipelineMode mode, List<String> stopwords) {
        super(ExecutorEnum.FilterDocuments, cap, mode);
        this.stopwords = stopwords;
    }

    public List<String> getStopwords() {
        return stopwords;
    }
}

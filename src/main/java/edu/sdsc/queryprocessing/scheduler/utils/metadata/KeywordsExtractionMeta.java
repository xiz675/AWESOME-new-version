package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

import java.util.List;

public class KeywordsExtractionMeta extends ExecutorMetaData {
    private List<String> keywords;
    public KeywordsExtractionMeta(List<String> keywords) {
        super(ExecutorEnum.KeywordsExtraction);
        this.keywords = keywords;
    }

    public KeywordsExtractionMeta(List<String> keywords, PipelineMode mode) {
        super(ExecutorEnum.KeywordsExtraction, PipelineMode.pipeline, mode);
        this.keywords = keywords;
    }

    public List<String> getKeywords() {
        return keywords;
    }
}

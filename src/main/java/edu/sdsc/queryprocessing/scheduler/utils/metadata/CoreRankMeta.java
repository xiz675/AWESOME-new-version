package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

import java.util.Map;
import java.util.Set;

public class CoreRankMeta extends ExecutorMetaData {
    private Map<Integer, Set<Integer>> neighbors;

    public CoreRankMeta(PipelineMode mode) {
        super(ExecutorEnum.Corerank, PipelineMode.pipeline, mode);
    }

    public CoreRankMeta(Map<Integer, Set<Integer>> neighbors, PipelineMode mode) {
        super(ExecutorEnum.Corerank, PipelineMode.pipeline, mode);
        this.neighbors = neighbors;
    }

    public void setNeighbors(Map<Integer, Set<Integer>> neighbors) {
        this.neighbors = neighbors;
    }

    public Map<Integer, Set<Integer>> getNeighbors() {
        return neighbors;
    }

}

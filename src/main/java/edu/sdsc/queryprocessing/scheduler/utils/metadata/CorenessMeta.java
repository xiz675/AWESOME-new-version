package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Set;

public class CorenessMeta extends ExecutorMetaData {
    private Map<Integer, Set<Integer>> neighbors;
    public CorenessMeta(PipelineMode mode) {
        super(ExecutorEnum.Coreness, PipelineMode.pipeline, mode);
    }

    public CorenessMeta(Map<Integer, Set<Integer>> neighbors, PipelineMode mode) {
        super(ExecutorEnum.Coreness, PipelineMode.pipeline, mode);
        this.neighbors = neighbors;
    }

    public void setGraph(Map<Integer, Set<Integer>> neighbors) {
        this.neighbors = neighbors;
    }

    public Map<Integer, Set<Integer>> getNeighbors() {
        return neighbors;
    }
}

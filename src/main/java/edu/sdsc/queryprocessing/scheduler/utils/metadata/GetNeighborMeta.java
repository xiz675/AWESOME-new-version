package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class GetNeighborMeta extends ExecutorMetaData {
    public GetNeighborMeta() {
        super(ExecutorEnum.GetNeighbor, PipelineMode.block, PipelineMode.block);
    }
}

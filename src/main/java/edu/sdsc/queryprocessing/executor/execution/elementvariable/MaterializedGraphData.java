package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.GraphElement;

import java.util.List;

public class MaterializedGraphData extends ExecutionTableEntryMaterialized<List<GraphElement>> {
    public MaterializedGraphData(List<GraphElement> edge) {
        setValue(edge);
    }

}

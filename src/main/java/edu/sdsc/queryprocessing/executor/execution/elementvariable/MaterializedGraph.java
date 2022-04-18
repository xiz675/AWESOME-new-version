package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.utils.Pair;
import org.neo4j.graphdb.GraphDatabaseService;

public class MaterializedGraph extends ExecutionTableEntryMaterialized<GraphDatabaseService> {
    public MaterializedGraph() {

    }
    public MaterializedGraph(GraphDatabaseService db) {
        this.setValue(db);
    }
}

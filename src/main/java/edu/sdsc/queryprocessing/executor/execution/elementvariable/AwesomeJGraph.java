package edu.sdsc.queryprocessing.executor.execution.elementvariable;


import org.jgrapht.Graph;

public class AwesomeJGraph extends ExecutionTableEntryMaterialized<Graph> {
    public AwesomeJGraph(Graph g) {
        setValue(g);
    }
}

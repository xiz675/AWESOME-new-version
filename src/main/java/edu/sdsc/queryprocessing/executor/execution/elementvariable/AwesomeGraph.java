package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import org.apache.tinkerpop.gremlin.structure.Graph;

public class AwesomeGraph extends ExecutionTableEntryMaterialized<Graph> {
  public AwesomeGraph(Graph g) {
    setValue(g);
  }
}

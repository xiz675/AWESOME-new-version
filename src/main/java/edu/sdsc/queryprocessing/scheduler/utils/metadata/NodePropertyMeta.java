package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;
import org.apache.tinkerpop.gremlin.structure.Graph;


public class NodePropertyMeta extends ExecutorMetaData{
    String propertyName;
    Graph graph;
    String valueType;

    public NodePropertyMeta(String name, String valueType, Graph g, PipelineMode mode) {
        super(ExecutorEnum.AddNodeProperty, PipelineMode.streaminput, mode);
        this.propertyName = name;
        this.valueType = valueType;
        this.graph = g;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }
}

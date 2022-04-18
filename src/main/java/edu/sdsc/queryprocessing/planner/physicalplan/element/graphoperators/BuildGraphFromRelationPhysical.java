package edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ConstructGraphFromRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.graph.CollectGraphElementFromRelation;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.*;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.physicalVar;

public class BuildGraphFromRelationPhysical extends PhysicalOperator {
    private String relationName;
    private Integer relationID;
    private boolean hasDirection;
    private String node1;
    private String node2;
    private String edge;
    private Map<String, String> node1Property = new HashMap<>();
    private Map<String, String> node2Property = new HashMap<>();
    private Map<String, String> edgeProperty = new HashMap<>();
    private Set<String> colNames = new HashSet<>();


    // todo:change this to ConstructGraphFromRelation
    // mark : cap on
    public BuildGraphFromRelationPhysical(ConstructGraphFromRelation ope) {
        this.setInputVar(physicalVar(ope.getInputVar()));
        this.relationName = ope.getRelationName();
        this.relationID = ope.getRelationUsed();
        JsonObject source = ope.getSource();
        JsonObject tgt = ope.getTarget();
        JsonObject edge = ope.getEdge();
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        this.setCapOnVarID(new Pair<>(this.relationID, "*"));

//        JsonObject detail = ope.getConstructionDetail();
//         = detail.getJsonObject("variable").getString("varName");

        this.hasDirection = ope.isHasDirection();
//        JsonObject edge = detail.getJsonObject("edge");
        List<Object> sourcePropertyWithValue = new ArrayList<>();
        List<Object> tgtPropertyWithValue = new ArrayList<>();
        List<Object> edgePropertyWithValue = new ArrayList<>();
        getProperties(edge.getJsonArray("property"), edgePropertyWithValue, this.edgeProperty);
        getProperties(source.getJsonArray("property"), sourcePropertyWithValue, this.node1Property);
        getProperties(tgt.getJsonArray("property"), tgtPropertyWithValue, this.node2Property);
        this.node1 = source.getJsonArray("label").getString(0);
        this.node2 = tgt.getJsonArray("label").getString(0);
        this.edge = edge.getJsonArray("label").getString(0);
        this.colNames.addAll(this.node1Property.values());
        this.colNames.addAll(this.node2Property.values());
        this.colNames.addAll(this.edgeProperty.values());
    }

    private void getProperties(JsonArray prop, List<Object> property, Map<String, String> unkownProp){
        for (JsonValue e : prop) {
            JsonObject temp = e.asJsonObject();
            if (temp.containsKey("value")) {
                property.add(temp.getString("name"));
                String type = temp.getString("type");
                if (type.equals("string")) {
                    property.add(temp.getString("value"));
                }
                else {
                    property.add(temp.getInt("value"));
                }
            }
            else {
                String rawCol = temp.getString("colName");
                String col = rawCol;
                String[] ss = rawCol.split("\\.");
                if (ss.length != 0) {
                    col = ss[1];
                }
                unkownProp.put(temp.getString("name"), col);}
        }

    }

    public Set<String> getColNames() {
        return colNames;
    }

    public String getNode1() {
        return node1;
    }

    public String getNode2() {
        return node2;
    }

    public String getEdge() {
        return edge;
    }

    public boolean isHasDirection() {
        return hasDirection;
    }

    public Map<String, String> getEdgeProperty() {
        return edgeProperty;
    }

    public Map<String, String> getNode1Property() {
        return node1Property;
    }

    public Map<String, String> getNode2Property() {
        return node2Property;
    }

    public String getRelationName() {
        return relationName;
    }

    public Integer getRelationID() {
        return relationID;
    }

    public void setRelationID(Integer relationID) {
        this.relationID = relationID;
    }

    public static void main(String[] args) {
//        JsonArrayBuilder nodeProp = Json.createArrayBuilder();
//        nodeProp.add(Json.createObjectBuilder().add("name", "k1").add("value", 1).add("type", "int").build());
//        nodeProp.add(Json.createObjectBuilder().add("name", "k2").add("value", "s").add("type", "string").build());
//        nodeProp.add(Json.createObjectBuilder().add("name", "k3").add("colName", "s").build());
//        List<Object> sourcePropertyWithValue = new ArrayList<>();
//        Map<String, String> node1Property = new HashMap<>();
//        getProperties(nodeProp.build(), sourcePropertyWithValue, node1Property);
//        System.out.println(sourcePropertyWithValue);
//        System.out.println(node1Property);

    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        CollectGraphElementFromRelation executor;
        Pair<Integer, String> inputVar = getInput(this);
        if (this.isStreamInput()) {
            Stream<AwesomeRecord> input;
            // check if direct value
            if (directValue) {
                input = (Stream<AwesomeRecord>) inputValue;
            }
            else {
                input = ((StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            // pipeline
            if (!this.isStreamOutput()) {
                executor = new CollectGraphElementFromRelation(input, this) ;
            }
            else {
                executor = new CollectGraphElementFromRelation(input, true, this) ;
            }
        }
        else {
            List<AwesomeRecord> input;
            if (directValue) {
                input = (List<AwesomeRecord>) inputValue;
            }
            else {
                input = ((MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new CollectGraphElementFromRelation(input, this);
            }
            else {
                executor = new CollectGraphElementFromRelation(input, true, this);
            }
        }

        return executor;
    }
}

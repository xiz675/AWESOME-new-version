//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.Edge;
//import edu.sdsc.datatype.execution.GraphElement;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.Node;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.InputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.CreateGraphPhysical;
//import edu.sdsc.utils.Pair;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.reactivestreams.Publisher;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.registerShutdownHook;
//import static edu.sdsc.utils.TinkerpopUtil.getRandomString;
//
//// todo: should add node property with value csvName and test it
//// this is a structure way to create a graph. Each graphElement should be an edge where each node should
//// have the same labels but different properties values
//// It is not flexible for any type of graphData List.
//// it stores all data to CSV and load from it for efficiency
//public class ExeCreateNeo4jGraphByCSV extends InputStreamExecutionBase {
//    private MaterializedGraphData materializedGraphData;
//    private StreamGraphData streamGraphData;
//    private String graphID;
//
//    public ExeCreateNeo4jGraphByCSV(CreateGraphPhysical ope, ExecutionVariableTable evt, String graphID) {
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        this.graphID = graphID;
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        if (mode.equals(PipelineMode.block)) {
//            this.materializedGraphData = ((MaterializedGraphData) evt.getTableEntry(inputVar));
//        }
//        else {
//            assert mode.equals(PipelineMode.streaminput);
//            this.streamGraphData = ((StreamGraphData) evt.getTableEntry(inputVar));
//        }
//    }
//
//
//    // this returns the csv tuple for the current graphElement
//    public List<Object> mapGraphData2CsvTuple(GraphElement g, List<String> node1Prop, List<String> node2Prop, List<String> edgeProp) {
//        assert g instanceof Edge;
//        List<Object> value = new ArrayList<>();
//        Edge edge = (Edge) g;
//        Node node1 = edge.getFirstNode();
//        Node node2 = edge.getSecondNode();
//        for (String p : node1Prop) {
//            value.add(node1.getProperty(p));
//        }
//        for (String p : node2Prop) {
//            value.add(node2.getProperty(p));
//        }
//        for (String p : edgeProp) {
//            value.add(g.getProperty(p));
//        }
//        return value;
//    }
//
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        List<GraphElement> graphData = materializedGraphData.getValue();
//        // todo: add an execution of the load csv file query
//        if (graphData.size() == 0) {
//            return null;
//        }
//        else {
//            // get CSV tuple and based on all CSV tuples, determine the csv header based on them
//            // node1 properties, edge properties, node2 properties
//            // for labels, assume that they are the same, and store all property values.
//            List<List> values = new ArrayList<>();
//            Edge edge = (Edge) graphData.get(0);
//            Node node1 = edge.getFirstNode();
//            Node node2 = edge.getSecondNode();
//            String edgeLabel = edge.getLabel();
//            String node1Label = node1.getLabel();
//            String node2Label = node2.getLabel();
//            List<String> node1PropertyKey = new ArrayList<>(node1.getProperties().keySet());
//            List<String> node2PropertyKey = new ArrayList<>(node2.getProperties().keySet());
//            List<String> edgePropertyKey =  new ArrayList<>(node2.getProperties().keySet());
//            for (GraphElement x : graphData) {
//                List<Object> value = mapGraphData2CsvTuple(x, node1PropertyKey, node2PropertyKey, edgePropertyKey);
//                values.add(value);
//            }
//            try {
//                String csvName = getRandomString(5);
//                FileWriter csvWriter = new FileWriter(csvName+".csv");
//                csvWriter.append(String.join(",", node1PropertyKey));
//                csvWriter.append(String.join(",", node2PropertyKey));
//                csvWriter.append(String.join(",", edgePropertyKey));
//                csvWriter.append("\n");
//                for (List<Object> rowData : values ) {
//                    csvWriter.append(String.join(",", rowData.toString()));
//                    csvWriter.append("\n");
//                }
//                csvWriter.flush();
//                csvWriter.close();
//                // create Neo4j Graph by loading from csv
//                createCypherQuery(csvName, node1Label, node2Label, edgeLabel, node1PropertyKey, node2PropertyKey, edgePropertyKey);
//                Path p = Paths.get("C://Users//xw//.Neo4jDesktop//relate-data//dbmss//dbms-5a4115c1-c15f-4dd5-8e49-e8dd688f54d3");
//                DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
//                registerShutdownHook(managementService);
////        managementService.startDatabase("neo4j");
//                GraphDatabaseService db = managementService.database("neo4j");
//                return new MaterializedGraph(db);
//            } catch (IOException e) {
//                e.printStackTrace();
//                return null;
//            }
//        }
//        // todo: should set a graph ID to distinguish this graph from others
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized executeStreamInput() {
//        Publisher<GraphElement> graphData = streamGraphData.getValue();
//
//        return null;
//    }
//
//    private String createCypherQuery(String csvName, String node1Label, String node2Label, String edgeLabel,
//                                     List<String> node1Prop, List<String> node2Prop, List<String> edgeProp)
//    {
//        String node1 = toCypherQuery("a", node1Label, node1Prop);
//        String node2 = toCypherQuery("b", node2Label, node2Prop);
//        String edge = toCypherQuery("", edgeLabel, edgeProp);
//        return String.format("LOAD CSV WITH HEADERS FROM \"%s.csv\" AS row\n" +
//                "MERGE (%s)\n" +
//                "MERGE (%s})\n" +
//                "CREATE (a)-[%s]->(b)", csvName, node1, node2, edge);
//    }
//
//    private String toCypherQuery(String name, String label, List<String> prop) {
//        if (label != null) {
//            name += (":" + label);
//        }
//        if (prop.size() > 0) {
//            List<String> temp = new ArrayList<>();
//            for (String p : prop) {
//                temp.add(p + ": row." + p);
//            }
//            name += String.join(",", temp);
//        }
//        return name;
//    }
//
//}

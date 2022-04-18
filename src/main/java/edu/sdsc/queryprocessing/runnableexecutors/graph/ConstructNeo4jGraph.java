package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.Edge;
import edu.sdsc.datatype.execution.GraphElement;
import edu.sdsc.datatype.execution.Node;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedGraph;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

// todo : this executor is hard coded
public class ConstructNeo4jGraph extends AwesomeStreamInputRunnable<GraphElement, GraphDatabaseService> {
    public ConstructNeo4jGraph(PipelineMode mode) {
        super(mode);
    }

    // blocking
    public ConstructNeo4jGraph(List<GraphElement> graph) {
        super(graph);
    }

    public ConstructNeo4jGraph(Stream<GraphElement> graph) {
        super(graph);
    }

    @Override
    public void executeStreamInput() {
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        getStreamInput().forEach(i -> mergeGraphHelper(mergedGraph, i));
        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
        storeGraph(graphEleSet);
    }

    @Override
    public void executeBlocking() {
        List<GraphElement> input = getMaterializedInput();
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        input.forEach(i -> mergeGraphHelper(mergedGraph, i));
        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
        storeGraph(graphEleSet);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedGraph(this.getMaterializedOutput());
    }

    public static void mergeGraphHelper(HashMap<GraphElement, GraphElement> mergedGraph, GraphElement i) {
        if (mergedGraph.containsKey(i)) {
            int count;
            Object temp = mergedGraph.get(i).getProperty("count");
            if (temp instanceof String) {
                count = Integer.parseInt((String) temp);
            }
            else {
                count = (Integer) temp;
            }
            mergedGraph.get(i).addProperty("count", count+1);
        }
        else {
            mergedGraph.put(i, i); }
    }

    public static void registerShutdownHook( final DatabaseManagementService managementService )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                managementService.shutdown();
            }
        } );
    }


    public static String getRegisteredProperty(String key) {
        Properties prop = new Properties();
        InputStream input = null;
        String result = null;
        try {
            input = new FileInputStream("config.properties");
            ;
            try {
                prop.load(input);
            } catch (IOException e) {
                e.printStackTrace();
            }

            result = prop.getProperty(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }

    private void storeGraph(List<GraphElement> graphEleSet) {
        Map<String, Long> nodes = new HashMap<>();
        DatabaseLayout dir = DatabaseLayout.ofFlat(Paths.get(getRegisteredProperty("neo4jdata")));
        Label nodeLable = Label.label("Word");
        RelationshipType relationType = RelationshipType.withName("cooccur");
        BatchInserter inserter = null;
        System.out.println("Create neo4j graph with edge size: " + graphEleSet.size());
        try {
            inserter = BatchInserters.inserter(dir);
            for (GraphElement ele : graphEleSet) {
                Node source = ((Edge) ele).getFirstNode();
                Node target = ((Edge) ele).getSecondNode();
                String sourceValue = (String) source.getProperty("value");
                String targetValue = (String) target.getProperty("value");
                long sourceNodeID;
                long targetNodeID;
                if (!nodes.containsKey(sourceValue)) {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put("value", sourceValue);
                    sourceNodeID = inserter.createNode(properties, nodeLable);
                    nodes.put(sourceValue, sourceNodeID);
                }
                else {
                    sourceNodeID = nodes.get(sourceValue);
                }
                if (!nodes.containsKey(targetValue)) {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put("value", targetValue);
                    targetNodeID = inserter.createNode(properties, nodeLable);
                    nodes.put(targetValue, targetNodeID);
                }
                else {
                    targetNodeID = nodes.get(targetValue);
                }
                Map<String, Object> edgeProp = new HashMap<>();
                if (ele.getProperty("count") == null) {
                    System.out.println("construct neo4j:" + sourceValue);
                    System.out.println(targetValue);
                }
                edgeProp.put("count", ele.getProperty("count"));
                inserter.createRelationship( sourceNodeID, targetNodeID, relationType, edgeProp );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if ( inserter != null )
            {
                inserter.shutdown();
            }
        }
        long startDB = System.currentTimeMillis();
        Path p = Paths.get(getRegisteredProperty("neo4j"));
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
//        registerShutdownHook(managementService);
//        managementService.startDatabase("neo4j");
        GraphDatabaseService db = managementService.database("neo4j");
        try ( Transaction tx = db.beginTx() ) {
            System.out.println("build index");
            long s = System.currentTimeMillis();
            // todo: change variable name
            String buildIndex = "CREATE INDEX valueindex IF NOT EXISTS FOR (n:Word) ON (n.value);\n";
            tx.execute(buildIndex);
            tx.commit();
            tx.close();
            long e = System.currentTimeMillis();
            System.out.println("finishes index");
            System.out.println("build index costs: " + (e-s));
        }
        setMaterializedOutput(db);
        System.out.println("-------start database (with/without index) costs: " + (System.currentTimeMillis() - startDB));

    }

    public static void main(String[] args) {
        List<GraphElement> elements = new ArrayList<>();
        Node a = new Node("word", "value", "xiuwen");
        Node b = new Node("word", "value", "dog");
        Node c = new Node("word", "value", "cat");
        Edge e1 = new Edge("cooccur", "count", 1);
        e1.setNodes(a, b, true);
        Edge e2 = new Edge("cooccur", "count", 2);
        e2.setNodes(a, c, true);
        elements.add(e1);
        elements.add(e2);
        ConstructNeo4jGraph graph = new ConstructNeo4jGraph(elements);
        graph.executeBlocking();
    }
}

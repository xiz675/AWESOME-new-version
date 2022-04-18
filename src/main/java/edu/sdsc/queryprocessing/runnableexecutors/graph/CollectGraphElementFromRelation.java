package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedGraphData;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamGraphData;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromRelationPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import org.neo4j.fabric.pipeline.FabricFrontEnd;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectGraphElementFromRelation extends AwesomePipelineRunnable<AwesomeRecord, GraphElement> {
    private Set<String> colName;
    private Map<String, String> node1Prop;
    private Map<String, String> node2Prop;
    private Map<String, String> edgeProp;
    private String node1Label;
    private String node2Label;
    private String edge;
    private boolean isHasDirection;
    private Integer unitSize = 500;
    private Integer producers = 1;

    // parallel
    public CollectGraphElementFromRelation(PipelineMode mode, BuildGraphFromRelationPhysical opt) {
        setExecutionMode(mode);
        this.colName = opt.getColNames();
        this.node1Prop = opt.getNode1Property();
        this.node2Prop = opt.getNode2Property();
        this.node1Label = opt.getNode1();
        this.node2Label = opt.getNode2();
        this.edge = opt.getEdge();
        this.edgeProp = opt.getEdgeProperty();
        this.isHasDirection = opt.isHasDirection();
    }

    // for blocking mode
    public CollectGraphElementFromRelation(List<AwesomeRecord> relation, boolean materializeOut, BuildGraphFromRelationPhysical opt) {
        this(PipelineMode.block, opt);
        this.setMaterializedInput(relation);
    }

    // for streamInput mode
    public CollectGraphElementFromRelation(Stream<AwesomeRecord> relation,  BuildGraphFromRelationPhysical opt) {
        this(PipelineMode.streaminput, opt);
        this.setStreamInput(relation);
    }

    // for streamOutput
    public CollectGraphElementFromRelation(List<AwesomeRecord> relation,  BuildGraphFromRelationPhysical opt) {
        this(PipelineMode.streamoutput, opt);
        this.setMaterializedInput(relation);
    }

    // pipeline
    public CollectGraphElementFromRelation(Stream<AwesomeRecord> relation, boolean streamOutput,  BuildGraphFromRelationPhysical opt) {
        this(PipelineMode.pipeline, opt);
        assert streamOutput;
        this.setStreamInput(relation);
    }

    private GraphElement matchToEdge(AwesomeRecord r) {
        Map<String, Object> colValues = new HashMap<>();
        Node node1 = new Node(node1Label);
        Node node2 = new Node(node2Label);
        for (String col : colName) {
            colValues.put(col, r.getColumn(col));
        }
        for (String key: node1Prop.keySet()) {
            node1.addProperty(key, colValues.get(node1Prop.get(key)));
        }
        for (String key: node2Prop.keySet()) {
            node2.addProperty(key, colValues.get(node2Prop.get(key)));
        }
        Edge crtEdge = new Edge(edge);
        for (String key: edgeProp.keySet()) {
            Object t = colValues.get(edgeProp.get(key));
            if (t == null) {
                System.out.println(node1.getProperty("value"));
                System.out.println(node2.getProperty("value"));
            }
            crtEdge.addProperty(key, colValues.get(edgeProp.get(key)));
        }
        crtEdge.setNodes(node1, node2, isHasDirection);
        return crtEdge;
    }


    @Override
    public void executeStreamInput() {
        Stream<AwesomeRecord> input = getStreamInput();
        Stream<GraphElement> result = input.map(this::matchToEdge);
        setMaterializedOutput(result.collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        Stream<GraphElement> result = getMaterializedInput().stream().map(this::matchToEdge);
        setStreamOutput(result);
    }

    @Override
    public void executePipeline() {
        Stream<AwesomeRecord> input = getStreamInput();
        Stream<GraphElement> result = input.map(this::matchToEdge);
        setStreamOutput(result);

//        try {
//            long start = System.currentTimeMillis();
//            System.out.println("collect graph element starts at: " + start + " ms");
//            long temp;
//            int times =1;
//            while (!isCancel()) {
//                // get peek, not remove it, so other consumers will reach here too
//                List<AwesomeRecord> t = input.peek();
//                if (t!= null && t.size()==0) {
//                    producers = producers - 1;
//                    if (producers==0) {
//                        streamResult.add(new ArrayList<>());
//                        setCancel(true);
//                        countDown();
//                    }
//                    else {
//                        // continue executing the later
//                        input.take();
//                    }
//                }
//                else if(t!=null) {
//                    input.take();
//                    List<GraphElement> graphEle = new ArrayList<>();
//                    for (AwesomeRecord r: t) {
//                        graphEle.add(matchToEdge(r));
//                    }
//                    streamResult.put(graphEle);
//                    temp = System.currentTimeMillis();
//                    System.out.println("add 5000 graphElements " + times + ": " + (temp - start) + " ms");
//                    times += 1;
//                    start = temp;
////                    System.out.println(Thread.currentThread().getName() + ": consume " + t);
//                }
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void executeBlocking() {
        List<AwesomeRecord> input = getMaterializedInput();
        List<GraphElement> output = new ArrayList<>();
        for (AwesomeRecord r: input) {
            output.add(matchToEdge(r));
        }
        setMaterializedOutput(output);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamGraphData(this.getStreamResult());
        }
        else {
            return new MaterializedGraphData(this.getMaterializedOutput());
        }
    }
}

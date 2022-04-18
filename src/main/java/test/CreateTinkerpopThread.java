package test;

import edu.sdsc.datatype.execution.GraphElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.opencypher.v9_0.expressions.functions.Count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;


public class CreateTinkerpopThread implements Runnable{
    private Graph g;
    private CountDownLatch latch;
    private List<GraphElement> data;

    public CreateTinkerpopThread(Graph g, CountDownLatch latch) {
        this.g = g;
        this.latch = latch;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread() + " starts at: " + System.currentTimeMillis());
//        for (GraphElement ele:this.data) {
//            addGraphElement(g, ele);
//        }
        createCoauthorGraph(g);
        System.out.println(Thread.currentThread() + " ends at: " + System.currentTimeMillis());
        latch.countDown();
    }

    public Graph getG() {
        return g;
    }

    private static void createCoauthorGraph(Graph graph) {
        try{
            BufferedReader buf = new BufferedReader(new FileReader("C:/Users/xw/Downloads/facebook_clean_data/artist_edges.csv"));
            String lineJustFetched = null;
            String[] wordsArray;
            GraphTraversalSource g = graph.traversal();
            Set<Integer> nodes = new HashSet<>();
            int count = 0;
            while(true){
                count = count + 1;
                lineJustFetched = buf.readLine();
                if(lineJustFetched == null || count >= 4000){
                    break;
                }
                else{
                    wordsArray = lineJustFetched.split(",");
                    Integer v1ID = Integer.valueOf(wordsArray[0]);
                    Optional<Vertex> v = g.V().has("id", v1ID).tryNext();
                    Vertex v1 = v.orElseGet(() -> graph.addVertex("id", v1ID));
                    Integer v2ID = Integer.valueOf(wordsArray[1]);
                    v = g.V().has("id", v2ID).tryNext();
                    Vertex v2 = v.orElseGet(() -> graph.addVertex("id", v2ID));
                    nodes.add(v1ID);
                    nodes.add(v2ID);
                    v1.addEdge("co-author", v2);
                }
            }
            buf.close();
            g.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}

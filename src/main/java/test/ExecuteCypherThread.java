package test;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.GraphElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.opencypher.gremlin.client.CypherGremlinClient;
import org.opencypher.gremlin.client.CypherResultSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;


public class ExecuteCypherThread implements Runnable{

    private Graph g;
    private CountDownLatch latch;


    public ExecuteCypherThread(CountDownLatch latch) {
        this.latch = latch;
    }

    public ExecuteCypherThread(Graph g, CountDownLatch latch) {
        this.g = g;
        this.latch = latch;
    }

    @Override
    public void run() {
//        System.out.println(Thread.currentThread() + " starts at: " + System.currentTimeMillis());
        long start = System.currentTimeMillis();
        GraphTraversalSource graph = this.g.traversal().withComputer();
//        CypherResultSet queryResult = executeInMemoryCypher("match (n)-[]-(m)-[]-(a) return m", graph);
//        GraphTraversalSource graph = this.g.traversal();
//        CypherGremlinClient cypherGremlinClient = CypherGremlinClient.inMemory(graph);
//        CypherResultSet result = cypherGremlinClient.submit("match (n)-[]-(m)-[]-(a) return m");
//        result.all();
//        cypherGremlinClient.close();
//
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        GraphTraversal<Vertex, Map<Object, Object>> x = graph.V().pageRank().by("gremlin.pageRankVertexProgram.pageRank")
                .elementMap("gremlin.pageRankVertexProgram.pageRank");
        List<Map<Object, Object>> y = x.toList();
        System.out.println(y.get(0).get("gremlin.pageRankVertexProgram.pageRank"));
        System.out.println(Thread.currentThread() + " execution: " + (System.currentTimeMillis() - start));
        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        latch.countDown();
    }
}

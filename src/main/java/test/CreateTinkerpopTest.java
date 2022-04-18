package test;

import edu.sdsc.datatype.execution.Edge;
import edu.sdsc.datatype.execution.GraphElement;
import edu.sdsc.datatype.execution.Node;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class CreateTinkerpopTest {
    public static List<GraphElement> createDataset(int size) {
        List<GraphElement> data = new ArrayList<>();
        for (int i=0; i<size; i++) {
            Node n1 = new Node(String.valueOf(2*i));
            Node n2 = new Node(String.valueOf(2*i + 1));
            Edge e = new Edge(String.valueOf(i));
            e.setNodes(n1, n2, false);
            data.add(e);
        }
        return data;
    }


    public static void main(String[] args) throws InterruptedException {
        int numThreads = 8;
        List<Graph> graphs = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i=0; i<numThreads; i++) {
            Graph g = TinkerGraph.open();
            CreateTinkerpopThread p = new CreateTinkerpopThread(g, latch);
            Thread t = new Thread(p);
            t.start();
            graphs.add(g);
        }
        latch.await();

        CountDownLatch latch2 = new CountDownLatch(numThreads);
        long start = System.currentTimeMillis();
        for (int i=0; i<numThreads; i++) {
            ExecuteCypherThread p = new ExecuteCypherThread(graphs.get(i), latch2);
            Thread t = new Thread(p);
            t.start();
        }
        latch2.await();
        long end = System.currentTimeMillis();
        System.out.println(String.format("execution time of %d core: %d", numThreads, end-start));


//        List<List> listOfData = partitionData(data, 8);
//        CountDownLatch latch = new CountDownLatch(8);
//        System.out.println("creation starts: " + System.currentTimeMillis());
//        for (int i=0; i<8; i++) {
//            CreateTinkerpopThread p = new CreateTinkerpopThread(g, listOfData.get(i), latch);
//            Thread t = new Thread(p);
//            t.start();
//        }
//        latch.await();
////        CreateTinkerpopThread p = new CreateTinkerpopThread(g, data, new CountDownLatch(1));
////        p.run();
//        System.out.println("creation ends: " + System.currentTimeMillis());
    }


}

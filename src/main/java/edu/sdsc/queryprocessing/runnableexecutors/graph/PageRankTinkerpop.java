package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opencypher.gremlin.translation.traversal.DeprecatedOrderAccessor.decr;

// each executor only accept input type and return output type, creating table entry is the main function's work.
public class PageRankTinkerpop extends AwesomeBlockRunnable<Graph, List<AwesomeRecord>> {
    private Integer numNodes;
    private boolean limitedNumber = false;

    public PageRankTinkerpop(Graph input) {
        super(input);
    }

    // for now, only consider blocking, no pipeline needs to be considered
    public PageRankTinkerpop(Graph input, Integer numNodes) {
        super(input);
        this.numNodes = numNodes;
        this.limitedNumber = true;
    }



    @Override
    public void executeBlocking() {
        System.out.println("page rank starts at " + System.currentTimeMillis());
        Graph graph = getMaterializedInput();
        GraphTraversalSource g = graph.traversal().withComputer();
        List<Map<Object, Object>> temp = new ArrayList<>();
        try {
            ComputerResult dcr = graph.compute().program(PageRankVertexProgram.build().create()).submit().get();
            if (limitedNumber) {
                temp = dcr.graph().traversal().V().order().by("gremlin.pageRankVertexProgram.pageRank", decr)
                        .limit(numNodes).valueMap("code", "gremlin.pageRankVertexProgram.pageRank").toList();
//                tempMap = g.V().pageRank().by("gremlin.pageRankVertexProgram.pageRank").limit(numNodes)
//                        .elementMap("gremlin.pageRankVertexProgram.pageRank");
            } else {
                temp = dcr.graph().traversal().V().order().by("gremlin.pageRankVertexProgram.pageRank", decr).
                        valueMap("code", "gremlin.pageRankVertexProgram.pageRank").toList();
//                tempMap = g.V().pageRank().elementMap("gremlin.pageRankVertexProgram.pageRank");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<AwesomeRecord> result = temp.stream().map(i -> {Map<String, Object> j = new HashMap<>(); j.put("id", i.get(T.valueOf("value")));
            j.put("pagerank", i.get("gremlin.pageRankVertexProgram.pageRank")); return j;}).map(AwesomeRecord::new).collect(Collectors.toList());
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedRelation(this.getMaterializedOutput());
    }

    public static void main(String[] args) {
        Graph graph = TinkerFactory.createModern();
        try {
            ComputerResult dcr = graph.compute().program(PageRankVertexProgram.build().create()).submit().get();
            List<Map<Object, Object>> x = dcr.graph().traversal().V().order().by("gremlin.pageRankVertexProgram.pageRank", decr)
                    .limit(10).valueMap("code", "gremlin.pageRankVertexProgram.pageRank").toList();
            System.out.println("s");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("success");
    }
}

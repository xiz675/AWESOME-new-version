package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;


import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.opencypher.gremlin.client.CypherResultSet;

import java.util.List;
import java.util.stream.Collectors;

import static edu.sdsc.utils.CypherUtils.getExecutionString;
import static edu.sdsc.utils.TinkerpopUtil.*;

// todo: we need to know which variable is graph variable
public class ExecuteCypherTinkerpop extends AwesomeStreamOutputRunnable<AwesomeGraph, AwesomeRecord> {
    //  private JsonObject config;
    private Graph graph;
    private String cypherStatement;
    //  private Connection toCon;

    // if the execution mode is materialize, then need to get the stream result input
    // if the mode is materialize then there is only one input variable and does not need to get the cypher statement
    public ExecuteCypherTinkerpop(Graph graph, ExecuteCypherPhysical ope,  ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        super(ope.getExecutionMode());
        this.graph = graph;
        this.cypherStatement = getExecutionString(ope.getStatement(), ope, false, evt,localEvt);
    }

    @Override
    public void executeStreamOutput() {
        System.out.println("executeCypher starts at " + System.currentTimeMillis());
        CypherResultSet queryResult = executeInMemoryCypher(cypherStatement, graph);
        setStreamResult(queryResult.stream().map(AwesomeRecord::new));
    }


    @Override
    public void executeBlocking() {
        System.out.println("executeCypher starts at " + System.currentTimeMillis());
        CypherResultSet queryResult = executeInMemoryCypher(cypherStatement, graph);
        List<AwesomeRecord> result = queryResult.stream().map(AwesomeRecord::new).collect(Collectors.toList());
        System.out.println("executeCypher result size: " + result.size());
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (!this.isStreamOut()) {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
        else {
            return new StreamRelation(this.getStreamResult());
        }
    }


    public static void main(String[] args) {

    }
}


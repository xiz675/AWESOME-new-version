package edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators;

import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ExecuteCypher;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.DBPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteCypherNeo4j;
import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteCypherTinkerpop;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.registerShutdownHook;
import javax.json.JsonArray;
import javax.json.JsonObject;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static edu.sdsc.utils.LoadConfig.getConfig;

public class ExecuteCypherPhysical extends DBPhysical {

    private Integer graphID = null ;
    private JsonArray schema;
    private boolean useDB = false;
    private GraphDatabaseService db = null;


    public ExecuteCypherPhysical(ExecuteCypher ch, Map<String, GraphDatabaseService> connectedDB) {
        String graphDBName = ch.getDbName();
        if (!graphDBName.equals("*")) {
            this.useDB = true;
            JsonObject t = getConfig("newsDB");
            String temp = t.getJsonObject(graphDBName).getString("path");
            System.out.println("neo4j path: " + temp);
            if (connectedDB.containsKey(temp)) {
                this.db = connectedDB.get(temp);
            }
            else {
                Path p = Paths.get(temp);
                DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
                registerShutdownHook(managementService);
                this.db = managementService.database("neo4j");
                connectedDB.put(temp, this.db);
            }
        }
        this.setGraphID(ch.getGraphID());
        this.setStatement(ch.getStatement());
        this.setVarDependent(ch.isVarDependent());
        this.setSchema(ch.getSchema());
        this.setPipelineCapability(PipelineMode.streamoutput);
        this.setParallelCapability(ParallelMode.sequential);
    }

    private void setGraphID(Integer gID) {
        this.graphID = gID;
    }

    public Integer getGraphID() {
        return graphID;
    }

    public void setSchema(JsonArray schema) {
        this.schema = schema;
    }

    public JsonArray getSchema() {
        return schema;
    }

    public boolean isUseDB() {
        return useDB;
    }

    public GraphDatabaseService getDb() {
        return db;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        // if graph is not a variable, is a Neo4j graph
        if (this.isUseDB()) {
            return new ExecuteCypherNeo4j(this.getDb(), this, evt, optimize, localEvt);
        }
        else {
            Integer gID = this.getGraphID();
            if (!directValue) {
                inputValue = getTableEntryWithLocal(new Pair<>(gID, "*"), evt, localEvt).getValue();
            }
            if (inputValue instanceof Graph) {
                return new ExecuteCypherTinkerpop((Graph) inputValue, this, evt, localEvt);
            }
            else {
                return new ExecuteCypherNeo4j((GraphDatabaseService) inputValue, this, evt, optimize, localEvt);
            }
        }
    }
}

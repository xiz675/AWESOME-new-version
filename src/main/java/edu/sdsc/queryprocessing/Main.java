/*
 * Copyright (c) 2019.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *
 */

package edu.sdsc.queryprocessing;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.mainexecution.ExecutionQuery;
import edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreatePhysicalPlan;
import edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreatePhysicalPlanWithPipeline;
import edu.sdsc.adil.Adil;
import edu.sdsc.queryprocessing.planner.logicalplan.CreatePlanDAG;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Operator;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.apache.commons.cli.*;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.jgrapht.Graph;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import javax.json.*;
import javax.json.stream.JsonGenerationException;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static edu.sdsc.queryprocessing.executor.execution.mainexecution.ExecutionQuery.parallelPipelineExecution;
import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.getRegisteredProperty;

public class Main {

    public static void main(String[] args) throws SQLException, IOException, JsonGenerationException,
            JsonMappingException, IOException {

        Options options = new Options();
        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        input = new Option("t", "input", true, "number of threads");
        input.setRequired(true);
        options.addOption(input);

        input = new Option("pip", "input", true, "if pipeline");
        input.setRequired(true);
        options.addOption(input);

        input = new Option("opt", "input", true, "if optimize");
        input.setRequired(true);
        options.addOption(input);


        input = new Option("gi", "input", true, "does it need initialize graph");
        input.setRequired(true);
        options.addOption(input);

        CommandLineParser parser = new DefaultParser();

        final CommandLine commandLine;
        String inputDir = null;
        String numThreads = null;
        String optimize = null;
        String pipeline = null;
        String useGraph = null;

        try {
            commandLine = parser.parse(options, args);
            inputDir = commandLine.getOptionValue("i");
            numThreads = commandLine.getOptionValue("t");
            pipeline = commandLine.getOptionValue("pip");
            optimize = commandLine.getOptionValue("opt");
            useGraph = commandLine.getOptionValue("gi");
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println("IO exceptions reading files " + e.getMessage());
        }

        Path path = Paths.get(inputDir);
        String stringFromFile = java.nio.file.Files.lines(path).collect(
                Collectors.joining());

        Reader sr = new StringReader(stringFromFile);
        JsonObjectBuilder js = Json.createObjectBuilder();

        Adil p = new Adil(sr);
        try {
            long startTime = System.currentTimeMillis();
            js = p.ADILStatement(js);
            VariableTable vtable = p.getVariableTable();
            // get the maximum variableID
            Integer varID = p.getVariableID();
            JsonObject config = p.getConfig();
            JsonObject x = js.build();
            long parseEndTime = System.currentTimeMillis();
            System.out.println("parsing takes: " + (parseEndTime - startTime) + " ms");
//            System.out.println(x);
            Connection inMemSQLCon = DriverManager.getConnection("jdbc:sqlite::memory:");
            inMemSQLCon.setAutoCommit(false);
            // if the underlying graph is used, needs to initialize it first
            if (Boolean.parseBoolean(useGraph)) {
                // start a database and then shut down
                Path np = Paths.get(getRegisteredProperty("neo4j"));
                DatabaseManagementService tempS = new DatabaseManagementServiceBuilder(np).build();
                GraphDatabaseService tempdb = tempS.database("neo4j");
                try ( Transaction tx = tempdb.beginTx() ) {tx.close();}
                tempS.shutdown();
            }
            // logical plan
            Graph<Operator, PlanEdge> plan = CreatePlanDAG.getNaivePlanDAG(x.getJsonArray("UnitAnalysis"), vtable);
            // physical plan
            // 1. the naive execution. set numThreads as 1
            // 2. parallel execution, set numThreads as 16
            List<List<PhysicalOperator>> physical = CreatePhysicalPlan.getPhysicalPlan(plan, vtable, config,true, Boolean.parseBoolean(optimize), false,Boolean.parseBoolean(pipeline));
            System.out.println("parse + plan time: " + (System.currentTimeMillis() - startTime) + " ms");
            ExecutionVariableTable vt = parallelPipelineExecution(physical, vtable, config, inMemSQLCon, Integer.parseInt(numThreads), Boolean.parseBoolean(optimize) );
            inMemSQLCon.close();
            long totalEndTime = System.currentTimeMillis();
//            System.out.println("map end time: " + totalEndTime + " ms");
            System.out.println("total time: " + (totalEndTime - startTime) + " ms");
            System.out.println("FIRST PASS [OK]");
        } catch (edu.sdsc.adil.ParseException pe) {
            System.out.println(pe.getLocalizedMessage());
        }
    }
}






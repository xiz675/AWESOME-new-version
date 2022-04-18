package edu.sdsc.queryprocessing.executor.execution.mainexecution;

import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeTextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromDocsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.HighLevelPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.storage.StoreList;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringFlatPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringReplacePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.FilterDocumentsPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.CompoundRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.ExeStoreList;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static edu.sdsc.queryprocessing.executor.utils.ExecutionUtil.*;
import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.*;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;


public class ExecutionQuery {
    // todo: add sequential execution

    // todo: add pipeline boolean variable, and add HLOs and make it executable
    // todo: for sub-operators of HLOs, skip them and will call them inside the HLOs

    // todo: add the code to insert to ET if needed

    // todo: there should be two functions that deal with different types of executions (HL and non-HL)



//    // the main execution code
//    // todo: Each parallelized operator is executed using multi cores and sequential one used single core
//    // todo: for the pipeline version, needs to have several single batch assignment and execute one SBA at one time
//    // for parallel execution, the results are always collected, so there are only three types of results, either a list of lists, or a list, or some non-list results types
//    public static ExecutionVariableTable parallelExecution(List<PhysicalOperator> operations, VariableTable vt, JsonObject config,
//                                                           Connection sqlCon, Integer numThreads, boolean optimize  ) {
//        // todo: Inside the HLO execution, should distinguish function operators and non-function operators
//        // todo: for sequential one, needs to determine if a merge is needed
//        ExecutionVariableTable variables = new ExecutionVariableTable();
//        long start;
//        long end;
//        for (PhysicalOperator opt : operations) {
//            // have two boolean one is to distinguish if this is a subOpe, another one is to distinguish if this is a function subOpe
////            if (opt.isReturnOperator()) {
////                continue;
////            }
//            // sub-operator will be called inside the HLO execution
//            if (opt.isSubOperator()) {
//                continue;
//            }
//            start = System.currentTimeMillis();
//            // todo: for LDA, does not need to change for now
//            if (opt instanceof LDAPhysical) {
//                Pair<TextMatrix, TextMatrix> result = multiOutputExecution(opt, vt, config, variables);
//                List<Pair<Integer, String>> output = ((LDAPhysical) opt).getOutputWithOrder();
//                int count = 0;
//                assert output.size() == 2;
//                variables.insertEntry(output.get(0), new AwesomeTextMatrix(result.first));
//                variables.insertEntry(output.get(1), new AwesomeTextMatrix(result.second));
//            }
//            else if (opt instanceof StoreList) {
//                ExeStoreList executor = (ExeStoreList) opt.createExecutor(variables, config, vt, sqlCon, optimize, null, false);
//                executor.run();
//            }
//            else if (opt instanceof HighLevelPhysical) {
//                ExecutionTableEntry result = hloExecution(numThreads, (HighLevelPhysical) opt, vt, config, variables, sqlCon, optimize);
//                Set<Pair<Integer, String>> output = opt.getOutputVar();
//                assert output.size() == 1;
//                variables.insertEntry(output.iterator().next(), result);
//            }
//            else {
//                ExecutionTableEntry result = parallelExecutionUnit(numThreads, opt,  variables, config, vt, sqlCon, optimize);
//                Set<Pair<Integer, String>> output = opt.getOutputVar();
//                assert output.size() == 1;
//                variables.insertEntry(output.iterator().next(), result);
//            }
//            end = System.currentTimeMillis();
//            // record time
//            if (opt instanceof MapPhysical) {
//                MapPhysical phy = (MapPhysical) opt;
//                System.out.println(String.format("Total execution time of %s of %s is %d ms", opt.getClass().getSimpleName(),
//                        phy.getInnerOpe().getClass().getSimpleName(), end-start));
//            }
//            else {
//                System.out.println(String.format("Total execution time of %s is %d ms", opt.getClass().getSimpleName(), end-start));
//            }
//            // todo: else call the unit function to get TE to insert
////            else if (opt instanceof FilterPhysical || opt instanceof BiOpePhysical) {
////
////            }
////            else if (opt instanceof SumPhysical || opt instanceof UnionListPhysical || opt instanceof ) {
////
////            }
////            else {
////                // parallel execution
////                ExecutionTableEntry result = parallelExecutionUnit(numThreads, opt, variables, config, operations, vt, sqlCon);
////                Set<Pair<Integer, String>> output = opt.getOutputVar();
////                assert output.size() == 1;
////                variables.insertEntry(output.iterator().next(), result);
////            }
//        }
//        return variables;
//    }

    // todo: finish the parallel + pipeline framework
    public static ExecutionVariableTable parallelPipelineExecution(List<List<PhysicalOperator>> operations, VariableTable vt, JsonObject config,
                                                           Connection sqlCon, Integer numThreads, boolean optimize) {

        ExecutionVariableTable variables = new ExecutionVariableTable();
        for (List<PhysicalOperator> opts:operations) {
            // have two boolean one is to distinguish if this is a subOpe, another one is to distinguish if this is a function subOpe
//            if (opt.isReturnOperator()) {
//                continue;
//          }
            // record the start time for each single pipeline
            long start = System.currentTimeMillis();
            assert opts.size() > 0;
            StringBuilder pipelineName = new StringBuilder();
            // todo: check if there is an operator in the chain that should be preprocessed, add preprocess for Map
            for (PhysicalOperator opt:opts) {
                pipelineName.append(opt.getClass().getSimpleName());
                if (opt instanceof MapPhysical) {
                    // some hard code
                    if (((MapPhysical) opt).getSubOperators().iterator().next() instanceof StringReplacePhysical) {
                        opt.setParallelCapability(ParallelMode.sequential);
                    }
                }
                if (opt instanceof FilterDocumentsPhysical || opt instanceof BuildGraphFromDocsPhysical || opt instanceof StringFlatPhysical || opt instanceof NERPhysical) {
                    preprocessForParallel(opt, variables);
                }
            }
            boolean isParallel = (opts.get(0).getParallelCapability().equals(ParallelMode.parallel));
            // only need to process it when it is parallel, needs to assign the materialized input for each first executor
            if (isParallel) {
                // get input and set direct value
                int trueNumThread;
                // get input var id
                Pair<Integer, String> inputVar = getInput(opts.get(0));
                // get input table entry from input operator and then partition data
                // since it is parallelized, the input should be materialized and should be a list
                ExecutionTableEntryMaterialized input = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(inputVar, variables);
                List<List> partitionedInput;
                // if the result is already partitioned, use it directly, else, partition it
                if (input.isPartitioned()) {
                    partitionedInput = (List<List>) input.getPartitionedValue();
                    trueNumThread = Math.min(partitionedInput.size(), numThreads);
                }
                else {
                    List inputData = (List) input.getValue();
                    if (inputData.size() == 0) {
                        trueNumThread = 1;
                    }
                    else {
                        trueNumThread = Math.min(inputData.size(), numThreads);
                    }
                    partitionedInput = partitionData(inputData, trueNumThread);
                }
                CountDownLatch latch = new CountDownLatch(trueNumThread);
                List<List> results = new ArrayList<>();
                List<Thread> threads = new ArrayList<>();
                List<CompoundRunnable> executors = new ArrayList<>();
                for (List i : partitionedInput) {
                    CompoundRunnable executor = new CompoundRunnable(opts, variables, config, vt, sqlCon, optimize, i, true, latch);
                    // todo : should add a boolean to create executor function to show if that is the head of the parallel pipeline
                    executors.add(executor);
                    threads.add(new Thread(executor));
                }
                for (Thread p : threads) {
                    p.start();
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Set<Pair<Integer, String>> output = opts.get(opts.size()-1).getOutputVar();
                // add TE to variable table
                for (CompoundRunnable exe : executors) {
                    results.add((List) exe.getResult());
                }
                // get the results from the each thread and then create a TE containing a list of collected results and add to the global table
                if (trueNumThread == 1) {
                    variables.insertEntry(output.iterator().next(), new ExecutionTableEntryMaterialized(results.get(0)));
                }
                else {
                    variables.insertEntry(output.iterator().next(), new ExecutionTableEntryMaterialized<List>(results, true));
                }
            }
            else {
                // create a single runnable
                CompoundRunnable executor = new CompoundRunnable(opts, variables, config, vt, sqlCon, optimize, null, false, new CountDownLatch(1));
                executor.run();
                PhysicalOperator lastOpe = opts.get(opts.size()-1);
                // create TE from the result of runnable and add to global VT
                // if LDA, should add two TEs
                if (lastOpe instanceof LDAPhysical) {
                    List<Pair<Integer, String>> output = ((LDAPhysical) lastOpe).getOutputWithOrder();
                    assert output.size() == 2;
                    List<ExecutionTableEntry> result =  executor.getResultsTE();
                    assert result.size() == 2;
                    variables.insertEntry(output.get(0), result.get(0));
                    variables.insertEntry(output.get(1), result.get(1));
                }
                else {
                    Set<Pair<Integer, String>> output = lastOpe.getOutputVar();
                    variables.insertEntry(output.iterator().next(), executor.getResultTE());
                }
            }
            long end = System.currentTimeMillis();
            System.out.printf("execution time for pipeline %s is %d ms%n",  pipelineName, (end-start));
        }
        return variables;
    }


//    public static ExecutionVariableTable execution(Graph<PhysicalOperator, PlanEdge> g, VariableTable vt, JsonObject config) throws SQLException, IOException, ClassNotFoundException, SolrServerException {
//        ExecutionVariableTable variables = new ExecutionVariableTable();
//        List<PhysicalOperator> operations = g.vertexSet().stream().sorted(Comparator.comparing(PhysicalOperator::getId)).collect(Collectors.toList());
//        for (PhysicalOperator opt : operations) {
//            if (opt instanceof ExecuteSQLPhysical) {
//                ExecuteSQLExecution exe = new ExecuteSQLExecution(config, (ExecuteSQLPhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof ColumnToList) {
//                ExecuteColToList exe = new ExecuteColToList(config, (ColumnToList) opt, variables);
//                exe.execute();
//
//            }
//            else if (opt instanceof UnionPhysical) {
//                ExecuteUnion exe = new ExecuteUnion(config, (UnionPhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof AutoPhrasePhysical) {
//                ExecuteAutoPhrase exe = new ExecuteAutoPhrase(config, (AutoPhrasePhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof ExecuteSolrPhysical) {
//                ExecuteSolrExecution exe = new ExecuteSolrExecution(config, (ExecuteSolrPhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof ExecuteCypherPhysical) {
//                ExecuteCypherNeo4j exe = new ExecuteCypherNeo4j(config, (ExecuteCypherPhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof Materialize) {
//                ExecuteMaterialize exe = new ExecuteMaterialize(config, (Materialize) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof NERPhysical) {
//                ExecuteNER exe = new ExecuteNER(config, (NERPhysical) opt, vt, variables);
//                exe.execute();
//            }
//            else if (opt instanceof HighLevelOperator) {
//
//            }
//        }
//        return variables;
//    }






}

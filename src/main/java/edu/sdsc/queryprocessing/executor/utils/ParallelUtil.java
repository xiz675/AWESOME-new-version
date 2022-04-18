package edu.sdsc.queryprocessing.executor.utils;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromDocsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromRelationPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringFlatPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.CreateDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.FilterDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.SplitDocumentsPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.graph.CollectGraphElementFromDocs;
import edu.sdsc.queryprocessing.runnableexecutors.graph.CollectGraphElementFromRelation;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.Map;
import edu.sdsc.queryprocessing.runnableexecutors.stringoperators.ExeStringFlat;
import edu.sdsc.queryprocessing.runnableexecutors.text.*;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static edu.sdsc.createdataset.Case1Dataset.readFile;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class ParallelUtil {

    // for parallel execution, needs to create multiple tasks for a single operator
    // for an operator which does not have parallel mode, it can't be applied this function
    // todo: add preprocessing for some operators like filterDoc and add some postprocessing for some operators like Sum and Union,
    //  need to add Matrix, put table entry creation outside and should let each executor takes actual value as input instead of table entry
    //  change the framework of other executors, each executor's input and output should be both data type instead of TE, and the creation and retrive from
    //  table entry should be done in this main function



    // partitioning input data for parallel execution
    public static List<List> partitionData(List input, int numThreads) {
        List<List> results = new ArrayList<>();
        Collections.shuffle(input);
        int subListSize = input.size()/numThreads;
        for (int i=0; i<numThreads; i++) {
            if (i==numThreads-1) {
                results.add(input.subList(i*subListSize, input.size()));
            }
            else {
                results.add(input.subList(i*subListSize, (i+1)*subListSize));
            }
        }
        return results;
    }

    public static<T> List<T> mergeData(List<List<T>> input) {
        List<T> result = new ArrayList<T>();
        for (List<T> i : input) {
            result.addAll(i);
        }
        return result;
    }

//    public static ExecutionTableEntry parallelExecutionHelper(Integer numThreads, PhysicalOperator opt, ExecutionVariableTable evt, JsonObject config,
//                                                              VariableTable vt, Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
//        int trueNumThread;
//        // get input var id
//        Pair<Integer, String> inputVar = getInput(opt);
//        // get input table entry from input operator and then partition data
//
//        // since it is parallelized, the input should be materialized and should be a list
//        ExecutionTableEntryMaterialized input = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(inputVar, evt, localEvt);
//        List<List> partitionedInput;
//        // if the result is already partitioned, use it directly, else, partition it
//        if (input.isPartitioned()) {
//            partitionedInput = (List<List>) input.getPartitionedValue();
//            trueNumThread = Math.min(partitionedInput.size(), numThreads);
//        }
//        else {
//            List inputData = (List) input.getValue();
//            if (inputData.size() == 0) {
//                trueNumThread = 1;
//            }
//            else {
//                trueNumThread = Math.min(inputData.size(), numThreads);
//            }
//            partitionedInput = partitionData(inputData, trueNumThread);
//        }
//        CountDownLatch latch = new CountDownLatch(trueNumThread);
////        int subListSize = input.size()/numThreads;
//        List<List> results = new ArrayList<>();
//        // preprocess some operators
//        if (opt instanceof FilterDocumentsPhysical || opt instanceof BuildGraphFromDocsPhysical || opt instanceof StringFlatPhysical || opt instanceof NERPhysical) {
////            FilterDocumentsPhysical phyOpe = (FilterDocumentsPhysical) opt;
////            phyOpe.setStopwords(readFile(phyOpe.getStopwordsFile()));
////            opt = phyOpe;
//            preprocessForParallel(opt, evt, localEvt);
//        }
//        long start = System.currentTimeMillis();
//        // todo: does every parallable operator is pipeline-able
//        List<AwesomePipelineRunnable> executors = new ArrayList<>();
//        List<Thread> threads = new ArrayList<>();
//        for (List i : partitionedInput) {
//            // todo : should add a boolean to create executor function to show if that is the head of the parallel pipeline
//            AwesomePipelineRunnable executor = makeASingleParallelExecutor(opt, evt, config, vt, sqlCon, optimize, localEvt);
//            executors.add(executor);
//            executor.setMaterializedInput(i);
////            executor.setMaterializedResult(output);
//            executor.setLatch(latch);
//            threads.add(new Thread(executor));
////            System.out.println(p1.getName());
//        }
//        for (Thread p : threads) {
//            p.start();
//        }
//        try {
//            latch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        for (AwesomePipelineRunnable exe : executors) {
//            results.add((List) exe.getMaterializedOutput());
//        }
////        if (opt instanceof MapPhysical && ((MapPhysical) opt).getInnerOpe() instanceof ExecuteCypherPhysical) {
////            ExecuteCypherPhysical temp = (ExecuteCypherPhysical) ((MapPhysical) opt).getInnerOpe();
////            Integer gID = temp.getGraphID();
////            ExecutionTableEntry te = getTableEntryWithLocal(new Pair<>(gID, "*"), evt, localEvt);
////            if (te instanceof MaterializedGraph) {
////                ((MaterializedGraph) te).getValue().
////            }
////
////        }
//        long end = System.currentTimeMillis();
//        System.out.println(String.format("Parallel execution time with core %d of %s is %d ms", trueNumThread, opt.getClass().getSimpleName(), (end-start)));
//        if (trueNumThread == 1) {
//            return new ExecutionTableEntryMaterialized(results.get(0));
//        }
//        // todo: for union and sum, add another thread to execute
//        // do not need to union, todo: when an operator is sequential, needs to determine if the result is partitioned, if so, need to merge
//        //    result first
//        else { return new ExecutionTableEntryMaterialized<List>(results, true);}
//    }




    public static void preprocessForParallel(PhysicalOperator opt, ExecutionVariableTable evt, ExecutionVariableTable... localEvt)
    {
        if (opt instanceof BuildGraphFromDocsPhysical) {
            BuildGraphFromDocsPhysical phyOpe = (BuildGraphFromDocsPhysical) opt;
            // get words
            FunctionParameter temp = phyOpe.getWords();
            if (temp != null) {
                phyOpe.setWordsValue((List<String>) temp.getValueWithExecutionResult(evt, localEvt));
            }
        }
        else if (opt instanceof StringFlatPhysical) {
            StringFlatPhysical phyOpe = (StringFlatPhysical) opt;
            if (phyOpe.getStringValue()==null) {
                phyOpe.setStringValue((String) phyOpe.getConcatStringID().getValueWithExecutionResult(evt, localEvt));
            }
        }
        else if (opt instanceof NERPhysical) {
            NERPhysical phyOpe = (NERPhysical) opt;
            try {
                InputStream in = new FileInputStream("config.properties");
                Properties con = new Properties();
                con.load(in);
                String modelPath = con.getProperty("default_classifier");
                phyOpe.setModel(modelPath);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
//        else if (opt instanceof StringJoinPhysical) {
//            StringJoinPhysical phyOpe = (StringJoinPhysical) opt;
//            if (phyOpe.getJoiner()==null) {
//                phyOpe.setJoiner((String) phyOpe.getStringListVar().getValueWithExecutionResult(evt, localEvt));
//            }
//            return phyOpe;
//        }
        else {
            assert opt instanceof FilterDocumentsPhysical;
            FilterDocumentsPhysical phyOpe = (FilterDocumentsPhysical) opt;
            phyOpe.setStopwords(readFile(phyOpe.getStopwordsFile()));
        }
    }

    // get the input variable of the operator, if the operator has more than one input, should
    // todo : what if the input is not a variable but constant?
    public static Pair<Integer, String> getInput(PhysicalOperator opt) {
        Pair<Integer, String> inputVar;
        if (opt instanceof BuildGraphFromDocsPhysical) {
            inputVar = ((BuildGraphFromDocsPhysical) opt).getDocID();
        }
        else if (opt instanceof BuildGraphFromRelationPhysical) {
            inputVar = new Pair<>(((BuildGraphFromRelationPhysical) opt).getRelationID(), "*");
        }
        else if (opt instanceof StringFlatPhysical) {
            FunctionParameter temp = ((StringFlatPhysical) opt).getStringList();
            inputVar = new Pair<>(temp.getVarID(), "*");
        }
//        else if (opt instanceof StringJoinPhysical) {
//            FunctionParameter temp = ((StringJoinPhysical) opt).getStringListVar();
//            inputVar = new Pair<>(temp.getVarID(), "*");
//        }
        else {
            inputVar = opt.getInputVar().iterator().next();
        }
        return inputVar;
    }



    // todo: for case1, does not need to parallelize union and sum, but need to do it in the future. In the outer function (parallelExecutionUnit),
    //  should call several threads of this, but also needs to do a merge
    // this is for parallelized operators and there should be another execution function that handles other operators such as LDA
//    private static AwesomePipelineRunnable makeASingleParallelExecutor(PhysicalOperator opt, ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
//        // todo: add all operators in the first case
//        if (opt instanceof CreateDocumentsPhysical) {
//            CreateDocumentsPhysical phyOpe = (CreateDocumentsPhysical) opt;
//            return new CreateDocuments(PipelineMode.block, phyOpe.docIDCol, phyOpe.textColName);
//        } else if (opt instanceof SplitDocumentsPhysical) {
//            SplitDocumentsPhysical phyOpe = (SplitDocumentsPhysical) opt;
//            return new SplitDocuments(PipelineMode.block, phyOpe.getSplitter());
//        } else if (opt instanceof FilterDocumentsPhysical) {
//            FilterDocumentsPhysical phyOpe = (FilterDocumentsPhysical) opt;
//            return new FilterDocuments(PipelineMode.block, phyOpe.getStopwords());
//        } else if (opt instanceof BuildGraphFromDocsPhysical) {
//            BuildGraphFromDocsPhysical phyOpe = (BuildGraphFromDocsPhysical) opt;
//            int dis = (int) phyOpe.getMaxDistance().getValueWithExecutionResult(evt, localEvt);
//            return new CollectGraphElementFromDocs(PipelineMode.block, dis, ((BuildGraphFromDocsPhysical) opt).getWordsValue());
//        } else if (opt instanceof BuildGraphFromRelationPhysical) {
//            return new CollectGraphElementFromRelation(PipelineMode.block, (BuildGraphFromRelationPhysical) opt);
//        }
//        else if (opt instanceof StringFlatPhysical) {
//            return new ExeStringFlat(PipelineMode.block, ((StringFlatPhysical) opt).getStringValue());
//        }
//        else if (opt instanceof NERPhysical) {
//            NERPhysical temp = (NERPhysical) opt;
//            return new ExeNER(PipelineMode.block, temp.getModel(), temp.getColName());
//        }
//        else {
//            MapPhysical phyOpe = (MapPhysical) opt;
//            return new Map(phyOpe, config, sqlCon, phyOpe.getLocalVarPairID(), optimize, evt, vt, phyOpe.getInnerOperators());
//        }
//    }

//
//        ExecutorEnum name = executor.getExecutor();
//        if (name.equals(ExecutorEnum.CreateDocuments)) {
//            CreateDocumentsMeta meta = (CreateDocumentsMeta) executor;
//            return new CreateDocuments(PipelineMode.block, meta.getDocID(), meta.getTextID());
//        }
//        else if (name.equals(ExecutorEnum.SplitDocuments)) {
//            SplitDocumentsMeta meta = (SplitDocumentsMeta) executor;
//            return new SplitDocuments(PipelineMode.block, meta.getSplitter());
//        }
//        else if (name.equals(ExecutorEnum.KeywordsExtraction)) {
//            KeywordsExtractionMeta meta = (KeywordsExtractionMeta) executor;
//            return new KeywordsExtraction(PipelineMode.block, meta.getKeywords());
//        }
//        else if (name.equals(ExecutorEnum.CollectCooccurance)) {
//            return new CollectCooccurance(PipelineMode.block);
//        }
//        else if (name.equals(ExecutorEnum.SleepTest)) {
//            return new Sleep(PipelineMode.block);
//        }
//        // todo: add high level operators
//        // todo: add all other parallelized operators
//        else {
//            assert name.equals(ExecutorEnum.CollectGraphElementFromListOfPairs);
//            return new CollectGraphElementFromListOfPairs(PipelineMode.block);
//        }

}

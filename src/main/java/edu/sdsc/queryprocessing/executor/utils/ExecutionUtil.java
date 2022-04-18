package edu.sdsc.queryprocessing.executor.utils;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.CompoundRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.BiOperationExecutor;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.FilterExecutor;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.BiOpePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.FilterPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.HighLevelPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringReplacePhysical;
import edu.sdsc.queryprocessing.runnableexecutors.text.*;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.*;

public class ExecutionUtil {
    // todo: need to change it, should have a uniform class for all, but currently it is fine
    // execution for HLOs
//    public static ExecutionTableEntry hloExecution(Integer num, HighLevelPhysical opt, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                   Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
//        // for filter operation, needs to deal with iterating (Text) matrix, need to create a new TM for each row or column
//        // for map, based on the execution mode of HLOs, choose stream one or materialized one
//        // add the local variable to table, and search it when execute it.
//        // if the sub-operator is not a function operator, just execute it once
//        // for map: there can be three types of variables to be iterated
//        // for each element variable, it must be materialized for the naive plan
//        // only map and filter can have other HLOs as subOperators, so only they may recursively call in their execution method
//        // todo: there can be nested high level operators where the input operator is also a HLO. Should add multiple localEvt like filter does
//        // for map, can execute in parallel, else can't
//        if (opt instanceof MapPhysical) {
//            // todo: should get a list of chains as the inner operators of the map
//            // get the element type and the inner operator
//            MapPhysical phyOpe = (MapPhysical) opt;
//            // set localVarPair
//
//            // todo: change this hard code thing
//            if (phyOpe.getSubOperators().iterator().next() instanceof StringReplacePhysical) {
//                num = 1;
//            }
//            return parallelExecutionHelper(num, phyOpe, evt, config, vt, sqlCon, optimize, localEvt);
//        }
//        // for filter
//        if (opt instanceof FilterPhysical) {
//            FilterPhysical filterP = (FilterPhysical) opt;
////            PhysicalOperator inner = findOpeByID(g, filterP.getInputOperator().iterator().next());
//            FilterExecutor filter = new FilterExecutor(filterP, evt, vt, config, sqlCon, localEvt);
//            filter.run();
//            return filter.getMaterializedOutput();
//        }
//        // todo: add reduce
//        // for reduce
//        // for BiOpe
//        else {
//            assert opt instanceof BiOpePhysical;
//            BiOpePhysical biOpeP = (BiOpePhysical) opt;
//            // for BiOpe, should also add type of the inner operator's output
//            PhysicalOperator left;
////                    findOpeByID(operators, biOpeP.getLeftOperator());
//            PhysicalOperator right;
////                    = findOpeByID(operators, biOpeP.getRightOperator());
//            BiOperationExecutor biOpe = new BiOperationExecutor(biOpeP, biOpeP.getLeftOpe(), biOpeP.getRightOpe(), evt, vt, config, sqlCon, localEvt);
//            biOpe.run();
//            return new AwesomeBoolean(biOpe.getMaterializedOutput());
//        }
//    }

    // this is for non-hlo
    // some operators can only have one threads and should be executed inside this function and while others can have more than one threads and should call helper function
//    public static ExecutionTableEntry parallelExecutionUnit(Integer numThreads, PhysicalOperator opt,  ExecutionVariableTable evt, JsonObject config,
//                                                            VariableTable vt, Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
//        if (opt.getParallelCapability().equals(ParallelMode.sequential)) {
//            return sequentialExecutionHelper(opt,  evt, config, vt, sqlCon, optimize, localEvt);
//        }
//        else {
//            return parallelExecutionHelper(numThreads, opt,  evt, config, vt, sqlCon, optimize, localEvt);
//        }
//        // after collecting result, need to form a table entry
//    }


    // todo: should separate it to multiple small functions
    private static ExecutionTableEntry sequentialExecutionHelper(PhysicalOperator opt, ExecutionVariableTable evt, JsonObject config,
                                                                 VariableTable vt, Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
        AwesomeRunnable executor = opt.createExecutor(evt, config, vt, sqlCon, optimize, null, false, localEvt);
        executor.run();
        return executor.createTableEntry();
    }


    public static Pair<TextMatrix, TextMatrix> multiOutputExecution(PhysicalOperator opt, VariableTable vt, JsonObject config, ExecutionVariableTable evt) {
//        FunctionExecutionBase executor;
        // todo: may add more operators which have multiple outputs
        assert opt instanceof LDAPhysical;
        LDAPhysical phyOpe = (LDAPhysical) opt;
        ExeLDA executor;
        // may need to merge
        Pair<Integer, String> docID = phyOpe.getDocID();
        ExecutionTableEntryMaterialized listInput = (ExecutionTableEntryMaterialized) evt.getTableEntry(docID);
        if (listInput.isPartitioned()) {
            List<List<Document>> tempValue = (List<List<Document>>) listInput.getPartitionedValue();
            List<Document> data = mergeData(tempValue);
            executor = new ExeLDA(phyOpe, evt, data);
        }
        else {
            executor = new ExeLDA(phyOpe, evt, (List<Document>) listInput.getValue());
        }
        executor.run();
        return executor.getMaterializedOutput();
    }


//    public static ExecutionTableEntry singleOutputExecution(PhysicalOperator opt, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                             Connection sqlCon, boolean optimize, ExecutionVariableTable... localEvt) {
//        if (opt instanceof HighLevelPhysical) {
//            return hloExecution(1, (HighLevelPhysical) opt, vt, config, evt, sqlCon, optimize, localEvt);
//        }
//        else {
//            return parallelExecutionUnit(1, opt, evt, config, vt, sqlCon, optimize, localEvt);
//        }
//    }

//    // todo: when fuse, make sure there is no sequential operations that needs to jump out of the Map, make sure all the dependent operators do not need to jump
//    //  outside the Map (or other HLOs)
//    public static ExecutionTableEntry executeDependentOperators(PhysicalOperator opt, VariableTable vt, JsonObject config,
//                                                                ExecutionVariableTable evt, Connection sqlCon,
//                                                                boolean optimize, ExecutionVariableTable localEvt) {
//        if (opt.isHasDependentOpe()) {
//            PhysicalOperator parent = findOpeByID(g, opt.getDependentOpeID());
//            ExecutionTableEntry parentResult = executeDependentOperators(parent, vt, config, evt, sqlCon, optimize, localEvt);
//            // for sub-operators of a Map operators, the result should be stored to this Map thread's local EVT
//            localEvt.insertEntry(parent.getOutputVar().iterator().next(), parentResult);
//            return singleOutputExecution(opt, vt, config, evt, sqlCon, optimize, localEvt);
//        }
//        else {
//            return singleOutputExecution(opt, vt, config, evt, sqlCon, optimize, localEvt);
//        }
//    }

    public static void insertResults(ExecutionVariableTable evt, CompoundRunnable executor, PhysicalOperator lastOpe) {
        if (lastOpe instanceof LDAPhysical) {
            LDAPhysical lda = (LDAPhysical) lastOpe;
            List<Pair<Integer, String>> output = lda.getOutputWithOrder();
            assert output.size() == 2;
            List<ExecutionTableEntry> results = executor.getResultsTE();
            evt.insertEntry(output.get(0), results.get(0));
            evt.insertEntry(output.get(1), results.get(1));
        }
        else {
            evt.insertEntry(lastOpe.getOutputVar().iterator().next(), executor.getResultTE());
        }
    }

    // this is for Map
    // todo: should add all
    public static ExecutionTableEntry createTableEntryForLocal(DataTypeEnum elementType, Object value) {
        ExecutionTableEntry result;
        if (elementType.equals(DataTypeEnum.Integer)) {
            result = new AwesomeInteger((Integer) value);
        }
        else if (elementType.equals(DataTypeEnum.Double)) {
            result = new AwesomeDouble((Double) value);
        }
        else if (elementType.equals(DataTypeEnum.Boolean)) {
            result = new AwesomeBoolean((boolean) value);
        }
        else if (elementType.equals(DataTypeEnum.TextMatrix)) {
            result = new AwesomeTextMatrix((TextMatrix) value);
        }
        else {
            assert elementType.equals(DataTypeEnum.String);
            result = new AwesomeString((String) value);
        }
        return result;
    }

    // todo: there should only be one situation where the directValue is true: the first operator of the chain which is a parallel operator
    //    needs to assign its portion to it
    // todo: based on the pipeline mode, should call different constructors of the same executor
    // this is to create a single executor, so if the input is partitioned, it should be merged
//    public static AwesomeRunnable createExecutor(List<PhysicalOperator> operators, PhysicalOperator opt,  ExecutionVariableTable evt, JsonObject config, VariableTable vt,
//                                          Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
//    }
//
//    // todo: should based on if the result is stream or not.  need to create both the materialized TE and stream TE since
//    //    the stream result will be added to localEVT to create executor for the next physical ope
//    // todo: needs to add all executors, should put this to each executor
//    public static ExecutionTableEntry createTableEntry(AwesomeRunnable executor) {
//        else if (executor instanceof PageRankJGraphT || executor instanceof PageRankTinkerpop) {
//            return new MaterializedRelation((List<AwesomeRecord>) executor.getMaterializedOutput());
//        }
//        else {
//            assert executor instanceof ExeUnionHLList;
//            return new MaterializedList(((ExeUnionHLList) executor).getMaterializedOutput());
//        }
//    }

}

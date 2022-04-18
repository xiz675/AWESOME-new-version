package edu.sdsc.queryprocessing.runnableexecutors.highleveloperators;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.CompoundRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static edu.sdsc.queryprocessing.executor.utils.ExecutionUtil.*;

// this is each thread of the Map executor, outside function should handle everything, for example, if the inner side create a tinkerpop graph or
// a sqlite table, should only have one thread
// todo: for the outside function, it should create TE for non-TE element
// todo: finish blocking first and then the rest
public class Map extends AwesomePipelineRunnable<ExecutionTableEntry, ExecutionTableEntry> {
    private List<PhysicalOperator> innerOpe;
    private Connection sqlCon = null;
    private Pair<Integer, String> localVar;
    private ExecutionVariableTable evt;
    private VariableTable vt;
    private JsonObject config;
    private DataTypeEnum elementType;
    private boolean optimize = false;
    private boolean pipeline = false;

    public Map(MapPhysical ope, JsonObject config, Connection sql, Pair<Integer, String> localVar,
               boolean optimize, boolean pipeline, ExecutionVariableTable evt, VariableTable vt) {
        super(ope.getExecutionMode());
        this.innerOpe = ope.getInnerOperators();
        this.config = config;
        this.localVar = localVar;
        this.evt = evt;
        this.vt = vt;
        this.sqlCon = sql;
        this.elementType = ope.getElementType();
        this.optimize = optimize;
        this.pipeline = pipeline;
    }


    // streamin/pipeline constructor
    public Map(MapPhysical ope, JsonObject config, Connection sql, Pair<Integer, String> localVar,
               boolean optimize, boolean pipeline, ExecutionVariableTable evt, VariableTable vt, Stream input) {
        this(ope,  config, sql, localVar, optimize, pipeline, evt, vt);
        this.setMapStreamInput(input);
    }

    // streamout/block constructor
    public Map(MapPhysical ope, JsonObject config, Connection sql, Pair<Integer, String> localVar,
               boolean optimize, boolean pipeline, ExecutionVariableTable evt, VariableTable vt, List input) {
        this(ope, config, sql, localVar, optimize, pipeline, evt, vt);
        this.setMapMaterializedInput(input);
    }
//
//    // pipeline constructor
//    public Map(PhysicalOperator ope, Stream<ExecutionTableEntry> input, boolean optimize, boolean streamOutput) {
//        super(input, true);
//        this.optimize = optimize;
//        assert streamOutput;
//        this.innerOpe = ope;
//    }
//
//    // block
//    public Map(PhysicalOperator ope, List<ExecutionTableEntry> input, boolean optimize, boolean materializeOutput) {
//        super(input, materializeOutput);
//        this.optimize = optimize;
//        assert materializeOutput;
//        this.innerOpe = ope;
//    }


    @Override
    public void executeStreamInput() {
        List<List<PhysicalOperator>> chains = setExecutionModeForMap(this.innerOpe, this.pipeline);
        List<ExecutionTableEntry> output = getStreamInput().map(te -> {
            ExecutionVariableTable localEvt = new ExecutionVariableTable();
            localEvt.insertEntry(this.localVar, te);
            return getTableEntryForMap(chains, vt, config, evt, sqlCon, optimize, localEvt);}).collect(Collectors.toList());
        setMaterializedOutput(output);
    }

    @Override
    public void executeStreamOutput() {
        List<List<PhysicalOperator>> chains = setExecutionModeForMap(this.innerOpe, this.pipeline);
        List<ExecutionTableEntry> input = getMaterializedInput();
        Stream<ExecutionTableEntry> output = input.stream().map(te -> {
            ExecutionVariableTable localEvt = new ExecutionVariableTable();
            localEvt.insertEntry(this.localVar, te);
            return getTableEntryForMap(chains, vt, config, evt, sqlCon, optimize, localEvt);});
        setStreamOutput(output);
    }

    @Override
    public void executePipeline() {
        List<List<PhysicalOperator>> chains = setExecutionModeForMap(this.innerOpe, this.pipeline);
        Stream<ExecutionTableEntry> input = getStreamInput();
        Stream<ExecutionTableEntry> output = input.map(te -> {
            ExecutionVariableTable localEvt = new ExecutionVariableTable();
            localEvt.insertEntry(this.localVar, te);
            return getTableEntryForMap(chains, vt, config, evt, sqlCon, optimize, localEvt);});
        setStreamOutput(output);
    }

    @Override
    public void executeBlocking() {
        List<List<PhysicalOperator>> chains = setExecutionModeForMap(this.innerOpe, this.pipeline);
        List<ExecutionTableEntry> input = getMaterializedInput();
        List<ExecutionTableEntry> result = new ArrayList<>();
        for (ExecutionTableEntry te : input) {
            // each Map thread should have a different local variable table
            ExecutionVariableTable localEvt = new ExecutionVariableTable();
            localEvt.insertEntry(this.localVar, te);
            result.add(getTableEntryForMap(chains, vt, config, evt, sqlCon, optimize, localEvt));
        }
        setMaterializedOutput(result);
    }

    // this is for only one thread
    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamHLCollection(this.getStreamResult());
        }
        else {
            return new MaterializedHLCollection(this.getMaterializedOutput());
        }
    }

    public void setSqlCon(Connection sqlCon) {
        this.sqlCon = sqlCon;
    }

    public void setLocalVar(Pair<Integer, String> localVar) {
        this.localVar = localVar;
    }

    public void setVt(VariableTable vt) {
        this.vt = vt;
    }

    public void setConfig(JsonObject config) {
        this.config = config;
    }

    public void setInnerOpe(List<PhysicalOperator> innerOpe) {
        this.innerOpe = innerOpe;
    }


    public void setEvt(ExecutionVariableTable evt) {
        this.evt = evt;
    }

//
//    // todo: incorporate it to executeUnit
//    private static ResultToStore getResultToStore(PhysicalOperator innerOpe, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                  List<PhysicalOperator> physicalGraph, Connection sqlCon, ExecutionVariableTable localEvt ) {
//        if (!innerOpe.isHasDependentOpe()){
////            System.out.println(Thread.currentThread().getName());
//            return singleOutputExecutionWithResultToStore(innerOpe, vt, config, evt, sqlCon, localEvt);
//        }
//        else {
//            // need to recursively execute dependent operator and insert them to table entry
//            return executeDependentOperatorsWithResultToStore(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//        }
//    }
//
//    private static ExecutionTableEntry getTableEntry(PhysicalOperator innerOpe, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                     Connection sqlCon, boolean optimize, ExecutionVariableTable localEvt) {
////        System.out.println(innerOpe.getClass() + ":" + Thread.currentThread().getName());
//        if (!innerOpe.isHasDependentOpe()){
//            return singleOutputExecution(innerOpe, vt, config, evt,  sqlCon, optimize, localEvt);
//        }
//        else {
//            // need to recursively execute dependent operator and insert them to table entry
//            return executeDependentOperators(innerOpe, vt, config, evt, sqlCon, optimize, localEvt);
//        }
//    }


    // write a new one for Map
    private static ExecutionTableEntry getTableEntryForMap(List<List<PhysicalOperator>> innerChains, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
                                                            Connection sqlCon, boolean optimize, ExecutionVariableTable localEvt) {
        long start;
        long end;
        for (int i=0; i<innerChains.size()-1; i++) {
            List<PhysicalOperator> crtChain = innerChains.get(i);
            // use localevt and global evt to create and run executor
            CompoundRunnable chainRunnable = new CompoundRunnable(crtChain, evt, config, vt, sqlCon, optimize, null, false, new CountDownLatch(1), localEvt);
//            start = System.currentTimeMillis();
            chainRunnable.run();
//            end = System.currentTimeMillis();
//            System.out.printf("execution time for pipeline %s is %d ms%n",  crtChain.stream().map(o -> o.getClass().getSimpleName()).collect(Collectors.joining(" ")), (end-start));
            // todo: add each chain's result to the local evt
            insertResults(localEvt, chainRunnable, crtChain.get(crtChain.size()-1));
            System.out.println();
        }
//        start = System.currentTimeMillis();
        List<PhysicalOperator> crtChain = innerChains.get(innerChains.size()-1);
        CompoundRunnable chainRunnable = new CompoundRunnable(crtChain, evt, config, vt, sqlCon, optimize, null, false, new CountDownLatch(1), localEvt);
        chainRunnable.run();
//        end = System.currentTimeMillis();
//        System.out.printf("execution time for pipeline %s is %d ms%n",  crtChain.stream().map(o -> o.getClass().getSimpleName()).collect(Collectors.joining(" ")), (end-start));
        return chainRunnable.getResultTE();
    }

    private static List<List<PhysicalOperator>> setExecutionModeForMap(List<PhysicalOperator> subOperators, boolean pipeline) {
        List<List<PhysicalOperator>> chains = new ArrayList<>();
        if (!pipeline) {
            for (PhysicalOperator p:subOperators) {
                List<PhysicalOperator> tempP = new ArrayList<>();
                tempP.add(p);
                chains.add(tempP);
            }
            return chains;
        }

        int size = subOperators.size();
        // for every operator, see if this was visited, if so, skip
        int i=0;
        while (i < size) {
            List<PhysicalOperator> chain = new ArrayList<>();
            PhysicalOperator curtOpe = subOperators.get(i);
            chain.add(curtOpe);
            int j = i+1;
            while (j < size) {
                PhysicalOperator nextOpe = subOperators.get(j);
                if ((curtOpe.getPipeCapability().equals(PipelineMode.streamoutput) || curtOpe.getPipeCapability().equals(PipelineMode.pipeline))
                        && (nextOpe.getPipeCapability().equals(PipelineMode.pipeline) || nextOpe.getPipeCapability().equals(PipelineMode.streaminput))) {
                    chain.add(nextOpe);
                    curtOpe = nextOpe;
                    j++;
                }
                else {
                    break;
                }
            }
            if (chain.size()>1) {
                chain.get(0).setExecutionMode(PipelineMode.streamoutput);
                chain.get(chain.size()-1).setExecutionMode(PipelineMode.streaminput);
                for (int ci=1; ci<chain.size()-1; ci++) {
                    // if this is the first operator in the list, then should set a streamOut mode
                    chain.get(ci).setExecutionMode(PipelineMode.pipeline);
                }
            } else {
                chain.get(0).setExecutionMode(PipelineMode.block);
            }
            chains.add(chain);
            i = j;
        }
        return chains;
    }





    // Map has its own setinput method
    public void setMapMaterializedInput(List materializedInput) {
        if (materializedInput.size()==0) {
            super.setMaterializedInput(new ArrayList<>());
        }
        else {
            if (!(materializedInput.get(0) instanceof ExecutionTableEntry)) {
                List<ExecutionTableEntry> tes = new ArrayList<>();
                for (Object i : materializedInput) {
                    tes.add(createTableEntryForLocal(elementType, i));
                }
                super.setMaterializedInput(tes);
            } else {
                super.setMaterializedInput(materializedInput);
            }
        }
    }

    public void setMapStreamInput(Stream streamInput) {
        Optional x = streamInput.findFirst();
        if (x.isEmpty()) {
            super.setStreamInput(streamInput);
        }
        else {
            if (!(x.get() instanceof ExecutionTableEntry)) {
                List<ExecutionTableEntry> tes = new ArrayList<>();
                super.setStreamInput(streamInput.map(i -> createTableEntryForLocal(elementType, i)));
            } else {
                super.setStreamInput(streamInput);
            }
        }
    }


}

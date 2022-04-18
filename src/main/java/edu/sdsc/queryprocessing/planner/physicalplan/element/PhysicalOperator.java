package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class PhysicalOperator {
    private Integer id;
    private Set<Pair<Integer, String>> inputVar = new HashSet<>();
    private Set<Pair<Integer, String>> outputVar = new HashSet<>();
    private String executeUnit = "*";
    private PipelineMode pipeCapability = PipelineMode.block;
    private ParallelMode paraCapability = ParallelMode.sequential;
    private Pair<Integer, String> capOnVarID = null;
    private boolean returnOperator = false;
    private Integer intermediateVarID = 0;
    private PipelineMode pipelineExeMode = PipelineMode.block;
    private boolean subOperator = false;
    private boolean hasDependentOpe = false;
    private Integer dependentOpeID = null;
    private boolean lastOpeInChain = false;
    private boolean firstOpeInChain = false;

    public abstract AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt,
                                                   Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt);

    public void setFirstOpeInChain(boolean firstOpeInChain) {
        this.firstOpeInChain = firstOpeInChain;
    }


    public void setLastOpeInChain(boolean lastOpeInChain) {
        this.lastOpeInChain = lastOpeInChain;
    }


    public void setHasDependentOpe(boolean hasDependentOpe) {
        this.hasDependentOpe = hasDependentOpe;
    }


    public void setDependentOpeID(Integer dependentOpeID) {
        this.setHasDependentOpe(true);
        this.dependentOpeID = dependentOpeID;
    }

    public Integer getDependentOpeID() {
        return dependentOpeID;
    }

    public boolean isSubOperator() {
        return subOperator;
    }

    public void setSubOperator(boolean subOperator) {
        this.subOperator = subOperator;
    }

    public PipelineMode getExecutionMode() {
        return pipelineExeMode;
    }

    public void setExecutionMode(PipelineMode executionMode) {
        this.pipelineExeMode = executionMode;
    }

    public void setIntermediateVarID(Integer intermediateVarID) {
        this.intermediateVarID = intermediateVarID;
    }

    public Integer getIntermediateVarID() {
        return intermediateVarID;
    }

    public PipelineMode getPipeCapability() {
        return pipeCapability;
    }

    public void setPipelineCapability(PipelineMode capability) {
        this.pipeCapability = capability;
    }

    public ParallelMode getParallelCapability() {return paraCapability;}

    public void setParallelCapability(ParallelMode capability) {this.paraCapability = capability;}

    public void setCapOnVarID(Pair<Integer, String> capOnVarID) {
        this.capOnVarID = capOnVarID;
    }

    public Pair<Integer, String> getCapOnVarID() {
        return capOnVarID;
    }

    public void setReturnOperator(boolean returnOperator) {
        this.returnOperator = returnOperator;
    }

    public boolean isReturnOperator() {
        return returnOperator;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setExecuteUnit(String executeUnit) {
        this.executeUnit = executeUnit;
    }

    public String getExecuteUnit() {
        return executeUnit;
    }

    public Set<Pair<Integer, String>> getInputVar() {
        return inputVar;
    }

    public Set<Pair<Integer, String>> getOutputVar() {
        return outputVar;
    }

    public void setInputVar(Set<Pair<Integer, String>>inputVar) {
        this.inputVar = inputVar;
    }

    public void setOutputVar(Set<Pair<Integer, String>>outputVar) {
        this.outputVar = outputVar;
    }

    public void addOutputVar(Pair<Integer, String> outputVar) {
        this.outputVar.add(outputVar);
    }

    public boolean isLastOpeInChain() {
        return lastOpeInChain;
    }

    public boolean isFirstOpeInChain() {
        return firstOpeInChain;
    }

    public boolean isHasDependentOpe() {
        return hasDependentOpe;
    }

    public boolean isStreamInput() {return pipelineExeMode.equals(PipelineMode.pipeline) || pipelineExeMode.equals(PipelineMode.streaminput);}

    public boolean isStreamOutput() {return pipelineExeMode.equals(PipelineMode.pipeline) || pipelineExeMode.equals(PipelineMode.streamoutput);}

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

}

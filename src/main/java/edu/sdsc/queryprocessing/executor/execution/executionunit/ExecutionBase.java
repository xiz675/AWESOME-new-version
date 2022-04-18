//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//
//public abstract class ExecutionBase<I, O> {
//
//    private PipelineMode executionMode = PipelineMode.block;
//    private I materializedInput;
//    private O materializedOutput;
//
//    public ExecutionBase() {}
//
//    public ExecutionBase(PipelineMode mode) {
//        this.executionMode = mode;
//    }
//
//    public PipelineMode getExecutionMode() {
//        return executionMode;
//    }
//
//    public void setMaterializedOutput(O materializedOutput) {
//        this.materializedOutput = materializedOutput;
//    }
//
//    public O getMaterializedOutput() {
//        return materializedOutput;
//    }
//
//    public void setMaterializedInput(I materializedInput) {
//        this.materializedInput = materializedInput;
//    };
//
//    public I getMaterializedInput() {
//        return materializedInput;
//    }
//
//    public abstract O executeWithMode();
//
//
////    ExecutionTableEntry executeWithMode();
//}

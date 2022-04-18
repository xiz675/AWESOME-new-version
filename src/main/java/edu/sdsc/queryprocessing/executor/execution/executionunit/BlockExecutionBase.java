//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.textmatrixoperations.GetValue;
//
//public abstract class BlockExecutionBase extends ExecutionBase<I, O> {
//    public void BlockExecutionBase() {
//        super(PipelineMode.block)
//    }
//
//    public PipelineMode getExecutionMode() {
//        return this.executionPipelineMode;
//    }
//
//    public ExecutionTableEntry executeWithMode() {
//        assert this.executionPipelineMode.equals(PipelineMode.block);
//        if (this instanceof ExecuteConstantAssign || this instanceof GetValue) {
//            return this.execute();
//        }
//        long startTime;
//        long endTime;
//        startTime = System.currentTimeMillis();
//        ExecutionTableEntry result = this.execute();
//        endTime = System.currentTimeMillis();
//        System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        return result;
//    }
//
//}

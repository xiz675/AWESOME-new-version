//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions.functionexecutionbase;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.ExecutionBase;
//
//import java.util.List;
//
//public abstract class FunctionBlockExecutionBase implements FunctionExecutionBase {
//    public abstract List<ExecutionTableEntryMaterialized> execute();
//    private PipelineMode executionMode;
//    public void setExecutionMode(PipelineMode executionMode) {
//        this.executionMode = executionMode;
//    }
//
//    public PipelineMode getExecutionMode() {
//        return this.executionMode;
//    }
//
//    public List<? extends ExecutionTableEntry> executeWithMode() {
//        assert this.executionMode.equals(PipelineMode.block);
//        List<? extends ExecutionTableEntry> result;
//        long startTime;
//        long endTime;
//        startTime = System.currentTimeMillis();
//        result = this.execute();
//        endTime = System.currentTimeMillis();
//        System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        return result;
////            return this.execute();
////        return this.execute();
//    }
//
//}

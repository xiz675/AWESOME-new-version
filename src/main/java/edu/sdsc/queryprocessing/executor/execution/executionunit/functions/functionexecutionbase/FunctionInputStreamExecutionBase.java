//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions.functionexecutionbase;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.ExecutionBase;
//
//import java.util.List;
//
//public abstract class FunctionInputStreamExecutionBase implements FunctionExecutionBase {
//    public abstract List<ExecutionTableEntryMaterialized> execute();
//    public abstract List<ExecutionTableEntryMaterialized> executeStreamInput();
//    private PipelineMode executionMode;
//
//    public void setExecutionMode(PipelineMode executionMode) {
//        this.executionMode = executionMode;
//    }
//    public PipelineMode getExecutionMode() {
//        return this.executionMode;
//    }
//
//    public List<? extends ExecutionTableEntry> executeWithMode() {
//        List<? extends ExecutionTableEntry> result;
//        long startTime;
//        long endTime;
//        if (executionMode.equals(PipelineMode.block)) {
//            startTime = System.currentTimeMillis();
//            result = this.execute();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
////            return this.execute();
//        }
//        else {
//            assert this.executionMode.equals(PipelineMode.streaminput);
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
////            return this.executeStreamInput();
//        }
//    }
//
//
//}

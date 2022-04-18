//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions.functionexecutionbase;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
//
//import java.util.List;
//
//public abstract class FunctionOutputStreamExecutionBase implements FunctionExecutionBase {
//    public abstract List<ExecutionTableEntryMaterialized> execute();
//    public abstract List<ExecutionTableEntryStream> executeStreamOutput();
//    public abstract List<ExecutionTableEntryMaterialized> materialize();
////    List<ExecutionTableEntryMaterialized> materialize(List<ExecutionTableEntryStream> input);
//
//    private PipelineMode executionPipelineMode;
//
//    public void setExecutionPipelineMode(PipelineMode executionPipelineMode) {
//        this.executionPipelineMode = executionPipelineMode;
//    }
//
//    public PipelineMode getExecutionPipelineMode() {
//        return this.executionPipelineMode;
//    }
//
//    public List<? extends ExecutionTableEntry> executeWithPipelineMode() {
//        List<? extends ExecutionTableEntry> result;
//        long startTime;
//        long endTime;
//        if (executionPipelineMode.equals(PipelineMode.block)) {
//            startTime = System.currentTimeMillis();
//            result = this.execute();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
////            return this.execute();
//        }
//        else if (executionPipelineMode.equals((PipelineMode.streamoutput))) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
////            return this.executeStreamOutput();
//        }
//        else {
//            assert executionPipelineMode.equals(PipelineMode.materializestream);
//            startTime = System.currentTimeMillis();
//            result = this.materialize();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
//        }
//
//    }
//
//}

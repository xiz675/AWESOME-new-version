//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
//
//public abstract class OutputStreamExecutionBase extends ExecutionBase {
//    private PipelineMode executionPipelineMode = PipelineMode.block;
//    public abstract ExecutionTableEntryMaterialized execute();
//    public abstract ExecutionTableEntryStream executeStreamOutput();
//
//
//
//
//    public void setExecutionMode(PipelineMode executionPipelineMode) {
//        this.executionPipelineMode = executionPipelineMode;
//    }
//
//    public PipelineMode getExecutionMode() {
//        return this.executionPipelineMode;
//    }
//
//    public ExecutionTableEntry executeWithMode() {
//        ExecutionTableEntry result;
//        long startTime;
//        long endTime;
//        if (executionPipelineMode.equals(PipelineMode.block)) {
//            startTime = System.currentTimeMillis();
//            result = this.execute();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else {
//            assert executionPipelineMode.equals((PipelineMode.streamoutput));
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        return result;
//    }
//}

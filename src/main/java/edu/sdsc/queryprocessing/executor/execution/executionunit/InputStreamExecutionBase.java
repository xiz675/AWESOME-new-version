//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//// import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//
//// in each class, should set the execution mode based on physical operator and then call the executionWithMode method.
//public abstract class InputStreamExecutionBase implements ExecutionBase {
//    private PipelineMode executionMode;
////    private ExecutionTableEntryStream streamInput;
////    private ExecutionTableEntryMaterialized materializedInput;
//
//    public abstract ExecutionTableEntryMaterialized execute();
//    public abstract ExecutionTableEntryMaterialized executeStreamInput();
//
//    public void setExecutionMode(PipelineMode executionMode) {
//        this.executionMode = executionMode;
//    }
//    public PipelineMode getExecutionMode() {
//        return this.executionMode;
//    }
//
////    public void setStreamInput(ExecutionTableEntryStream streamInput) {
////        this.streamInput = streamInput;
////    }
////
////    public ExecutionTableEntryStream getStreamInput() {
////        return streamInput;
////    }
////
////    public void setMaterializedInput(ExecutionTableEntryMaterialized materializedInput) {
////        this.materializedInput = materializedInput;
////    }
////
////    public ExecutionTableEntryMaterialized getMaterializedInput() {
////        return materializedInput;
////    }
//
//    public ExecutionTableEntry executeWithMode() {
//        ExecutionTableEntry result;
//        long startTime;
//        long endTime;
//        if (this.executionMode.equals(PipelineMode.block)) {
//            startTime = System.currentTimeMillis();
//            result = this.execute();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else {
//            assert this.executionMode.equals(PipelineMode.streaminput);
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        return result;
//    }
//
//    public static void main(String[] args) {
//        Object s;
//        s = new String("s");
//        System.out.println(s.getClass().getSimpleName());
//    }
//}

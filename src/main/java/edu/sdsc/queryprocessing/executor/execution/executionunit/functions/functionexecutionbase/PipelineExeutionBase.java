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
//public abstract class PipelineExeutionBase implements FunctionExecutionBase {
//    public abstract List<ExecutionTableEntryMaterialized> execute();
//    public abstract List<ExecutionTableEntryMaterialized> executeStreamInput();
//    public abstract List<ExecutionTableEntryStream> executeStreamOutput();
//    public abstract List<ExecutionTableEntryStream> executeStreamInputStreamOutput();
//    public abstract List<ExecutionTableEntryMaterialized> materialize();
//    private PipelineMode executionPipelineMode;
//    public void setExecutionPipelineMode(PipelineMode executionPipelineMode) {
//        this.executionPipelineMode = executionPipelineMode;
//    }
//
//    public PipelineMode getExecutionPipelineMode() {
//        return this.executionPipelineMode;
//    }
//
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
//        else if (executionPipelineMode.equals((PipelineMode.streaminput))) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//            return result;
////            return this.executeStreamOutput();
//        }
//        else if (executionPipelineMode.equals((PipelineMode.pipeline))) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInputStreamOutput();
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
//    }
//}

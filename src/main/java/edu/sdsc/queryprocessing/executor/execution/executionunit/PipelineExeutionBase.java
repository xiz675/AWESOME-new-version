//package edu.sdsc.queryprocessing.executor.execution.executionunit;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
//
//public abstract class PipelineExeutionBase implements ExecutionBase{
//    private PipelineMode executionMode;
//    public abstract ExecutionTableEntryMaterialized execute();
//    public abstract ExecutionTableEntryMaterialized executeStreamInput();
//    public abstract ExecutionTableEntryStream executeStreamOutput();
//    public abstract ExecutionTableEntryStream executeStreamInputStreamOutput();
//
//    public void setExecutionMode(PipelineMode executionMode) {
//        this.executionMode = executionMode;
//    }
//
//    public PipelineMode getExecutionMode() {
//        return this.executionMode;
//    }
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
//        else if (this.executionMode.equals(PipelineMode.streaminput)) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else if (this.executionMode.equals(PipelineMode.streamoutput)) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else {
//            assert this.executionMode.equals(PipelineMode.pipeline);
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInputStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//
//        }
//        return result;
//    }
//}

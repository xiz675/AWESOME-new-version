package edu.sdsc.queryprocessing.runnableexecutors;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;

import java.util.ArrayList;
import java.util.List;

public class Sleep  extends AwesomePipelineRunnable<Integer, Integer> {

    public Sleep(PipelineMode mode) {
        super(mode);
    }

    @Override
    public void executeStreamInput() {

    }

    @Override
    public void executeStreamOutput() {

    }

    @Override
    public void executePipeline() {

    }

    @Override
    public void executeBlocking() {
        List<Integer> materializedInput = getMaterializedInput();
        List<Integer> materializedResult = new ArrayList<>();
        try {
            for (Integer i : materializedInput) {
                Thread.sleep(100);
                materializedResult.add(i);
            }
            countDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        setMaterializedOutput(materializedResult);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }
}

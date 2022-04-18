package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.datatype.execution.PipelineMode;

public abstract class AwesomeBlockRunnable<I, O> extends AwesomeRunnable<I, O>{

    public AwesomeBlockRunnable(I input) {
        super(PipelineMode.block);
        setMaterializedInput(input);
    }

    @Override
    public void run() {
        executeBlocking();
    }

}

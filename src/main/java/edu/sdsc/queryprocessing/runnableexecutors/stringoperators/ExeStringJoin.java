package edu.sdsc.queryprocessing.runnableexecutors.stringoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeString;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;

import java.util.List;

public class ExeStringJoin extends AwesomeBlockRunnable<List<String>, String> {
    private String joiner;

    public ExeStringJoin(List<String> input, String joiner) {
        super(input);
        this.joiner = joiner;
    }

    @Override
    public void executeBlocking() {
        setMaterializedOutput(String.join(joiner, this.getMaterializedInput()));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeString(this.getMaterializedOutput());
    }
}

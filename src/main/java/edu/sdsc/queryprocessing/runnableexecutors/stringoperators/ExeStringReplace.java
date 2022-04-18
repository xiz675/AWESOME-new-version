package edu.sdsc.queryprocessing.runnableexecutors.stringoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeString;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.utils.Pair;

public class ExeStringReplace extends AwesomeBlockRunnable<Pair<String, String>, String> {
    private String replacedString;
    private String value;

    public ExeStringReplace(Pair<String, String> s) {
        super(s);
        replacedString = s.first;
        value = s.second;
    }

    @Override
    public void executeBlocking() {
        String s = replacedString.replaceAll("\\$", value);
        setMaterializedOutput(s);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeString(this.getMaterializedOutput());
    }
}

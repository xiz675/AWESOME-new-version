package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeDouble;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;

import java.util.List;
import java.util.stream.Stream;

// todo: for now only accept and return double
public class SumList extends AwesomeStreamInputRunnable<Object, Double> {

    public SumList(PipelineMode mode) {
        super(mode);
    }

    public SumList(List<Object> input) {
        super(input);
    }

    public SumList(Stream<Object> input) {
        super(input);
    }


    private static double castObject(Object x) {
        if (x instanceof String) {
            return  Double.parseDouble((String) x);
        }
        else if (x instanceof Integer) {
            return (double) x;
        }
        else {
            assert x instanceof Double;
            return (Double) x;
        }
    }

    @Override
    public void executeStreamInput() {
        setMaterializedOutput(getStreamInput().map(SumList::castObject).reduce(Double::sum).get());
    }

    @Override
    public void executeBlocking() {
        setMaterializedOutput(getMaterializedInput().stream().map(SumList::castObject).reduce(Double::sum).get());
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeDouble(this.getMaterializedOutput());
    }

}

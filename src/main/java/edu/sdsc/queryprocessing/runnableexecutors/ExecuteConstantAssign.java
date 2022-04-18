package edu.sdsc.queryprocessing.runnableexecutors;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ConstantAssignmentPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;

import java.util.List;

public class ExecuteConstantAssign extends AwesomeBlockRunnable<ConstantAssignmentPhysical, ExecutionTableEntryMaterialized> {

    public ExecuteConstantAssign(ConstantAssignmentPhysical ope) {
        super(ope);
    }

    @Override
    public void executeBlocking() {
        Object value = getMaterializedInput().getValue();
        if (value instanceof Boolean) {
            setMaterializedOutput(new AwesomeBoolean((boolean) value));
        }
        else if (value instanceof Double) {
            setMaterializedOutput(new AwesomeDouble((double) value));
        }
        else if (value instanceof Integer) {
            setMaterializedOutput(new AwesomeInteger((Integer) value));
        }
        else if (value instanceof String) {
            setMaterializedOutput(new AwesomeString((String) value));
        }
        else {
            // todo: it is not needed now but may be needed in the future
            assert value instanceof List;
            setMaterializedOutput(new MaterializedList<Object>((List<Object>) value));
        }
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return this.getMaterializedOutput();
    }

}
package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.datatype.execution.TextMatrix;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.GetTextMatrixValuePhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// todo: only supports double for now
public class GetValue extends AwesomeBlockRunnable<TextMatrix, Double> {
    public double execute(TextMatrix tm, Integer index) {
        return tm.getValue().get(index);
    }
    private Integer index;

    public GetValue(TextMatrix tm, Integer index) {
        super(tm);
        this.index = index;
    }

    @Override
    public void executeBlocking() {
        double value = getMaterializedInput().getValue().get(this.index);
        setMaterializedOutput(value);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeDouble(this.getMaterializedOutput());
    }
}
//
//if (opt.isHasIndex()) {
//        index = opt.getIndex();
//        }
//        else{
//        index = ((AwesomeInteger) getTableEntryWithLocal(opt.getIndexID(), evt, localEvt)).getValue();
//        }
//tm = (TextMatrix) getTableEntryWithLocal(opt.getTmID(), evt, localEvt).getValue();

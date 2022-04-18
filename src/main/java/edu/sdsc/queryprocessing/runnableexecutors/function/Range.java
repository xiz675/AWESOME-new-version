package edu.sdsc.queryprocessing.runnableexecutors.function;


import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ListCreationPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import edu.sdsc.utils.Pair;
import io.reactivex.rxjava3.core.Flowable;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class Range extends AwesomeStreamOutputRunnable<ListCreationPhysical, Integer> {
    private final Integer start;
    private final Integer end;
    private final Integer step;

    // this can be kept, if this is called as an operator, it is fine. If this is called as a suboperator, some other steps need to be done
    public Range(ListCreationPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        super(ope.getExecutionMode());
        this.start = getValue(ope.getStart(), evt, localEvt);
        this.end = getValue(ope.getEnd(), evt, localEvt);
        this.step = getValue(ope.getStep(), evt, localEvt);
    }

    private Integer getValue(Pair<Boolean, Integer> value, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        if (value.first) {
            return value.second;
        }
        else {
            return ((AwesomeInteger) getTableEntryWithLocal(new Pair<>(value.second, "*"), evt, localEvt)).getValue();
        }
    }

    private Stream<Integer> intervalSequence()
    {
        return Stream.iterate(start, i -> i < end, i -> step + i);
    }

//    public Publisher<Integer> getRangeList() {
//        return intervalSequence(this.start,  this.step, this.end);
//    }

//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        this.streamResult = executeStreamOutput();
//        return materialize();
//    }
//
//    @Override
//    public StreamList<Integer> executeStreamOutput() {
//        return new StreamList<>(intervalSequence(this.start,  this.step, this.end));
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized materialize() {
//        return new MaterializedList<>(Flowable.fromPublisher(streamResult.getValue()).toList().blockingGet());
//    }

    @Override
    public void executeBlocking() {
        setMaterializedOutput(intervalSequence().collect(Collectors.toList()));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamList<>(this.getStreamResult());
        }
        else {
            return new MaterializedList<>(this.getMaterializedOutput());
        }
    }

    @Override
    public void executeStreamOutput() {
        setStreamResult(intervalSequence());
    }
}
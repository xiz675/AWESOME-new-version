//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.ListCreationPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import org.reactivestreams.Publisher;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class Range extends OutputStreamExecutionBase {
//    private Integer start;
//    private Integer end;
//    private Integer step;
//    private StreamList<Integer> streamResult;
//
//    // this can be kept, if this is called as an operator, it is fine. If this is called as a suboperator, some other steps need to be done
//    public Range(ListCreationPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        this.start = getValue(ope.getStart(), evt, localEvt);
//        this.end = getValue(ope.getEnd(), evt, localEvt);
//        this.step = getValue(ope.getStep(), evt, localEvt);
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//    }
//
////    public Range(OperatorWithMode ope, ExecutionVariableTable evt) {
////
////    }
//
//    private Integer getValue(Pair<Boolean, Integer> value, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        if (value.first) {
//            return value.second;
//        }
//        else {
//            return ((AwesomeInteger) getTableEntryWithLocal(new Pair<>(value.second, "*"), evt, localEvt)).getValue();
//        }
//    }
//
//    private Publisher<Integer> intervalSequence(int start, int step, int end)
//    {
//        return Flowable.generate(()->start,
//                (s, emitter)->{
//                    if (s + step > end ) {
//                        emitter.onComplete();}
//                    int next = s+step;
//                    emitter.onNext(next);
//                    return next;
//                });
//    }
//
////    public Publisher<Integer> getRangeList() {
////        return intervalSequence(this.start,  this.step, this.end);
////    }
//
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
//}

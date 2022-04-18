//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions.sum;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.InputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.SumPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//
//import java.util.List;
//import java.util.stream.Stream;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class SumList extends InputStreamExecutionBase {
//    private Stream<Object> streamInput;
//    private List<Object> listInput;
//    //todo: check if the constructor is correct with the physical operator
//
//    public SumList() {
//        this.setExecutionMode(PipelineMode.block);
//    }
//
//    public SumList(SumPhysical opt, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        Pair<Integer, String> inputVar = opt.getSumVarID();
//        this.setExecutionMode(PipelineMode.streaminput);
//        PipelineMode mode = opt.getExecutionMode();
//        if (mode.equals(PipelineMode.streaminput)) {
//            this.streamInput = (Stream<Object>) getTableEntryWithLocal(inputVar, evt, localEvt).getValue();
//        }
//        else {
//            this.listInput = (List<Object>) getTableEntryWithLocal(inputVar, evt, localEvt).getValue();
//        }
//    }
//
//    private static double castObject(Object x) {
//        if (x instanceof String) {
//            return  Double.parseDouble((String) x);
//        }
//        else if (x instanceof Integer) {
//            return (double) x;
//        }
//        else {
//            assert x instanceof Double;
//            return (Double) x;
//        }
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        double rsult = 0;
//        for (Object i : this.listInput) {
//            rsult += castObject(i);
//        }
//        return new AwesomeDouble(rsult);
//    }
//
//
//    @Override
//    public ExecutionTableEntryMaterialized executeStreamInput() {
//        return new AwesomeDouble(streamInput.map(SumList::castObject).reduce(Double::sum).get());
//    }
//
//    public void setMaterializedInput(List<Object> materializedInput) {
//        this.listInput = materializedInput;
//    }
//
//}

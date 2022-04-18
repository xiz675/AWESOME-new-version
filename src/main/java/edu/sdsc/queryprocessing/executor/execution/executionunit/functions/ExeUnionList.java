//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.Materialize;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.UnionListPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//import org.reactivestreams.Publisher;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.stream.Stream;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//// the input should be a list of lists
//public class ExeUnionList extends InputStreamExecutionBase {
//    private List<List<Object>> materializedValue = new ArrayList<>();
//    private Stream streamList;
//    private boolean isHL = false;
//
//    public ExeUnionList() {setExecutionMode(PipelineMode.block);}
//
//    public ExeUnionList(UnionListPhysical opt, ExecutionVariableTable evt, ExecutionVariableTable[] localEvt) {
//        PipelineMode mode = opt.getExecutionMode();
//        this.setExecutionMode(mode);
//        Pair<Integer, String> listVar = opt.getListVarID();
//        if (mode.equals(PipelineMode.streaminput)) {
//            ExecutionTableEntry temp = getTableEntryWithLocal(listVar, evt, localEvt);
//            if (temp instanceof StreamList) {
//                this.streamList = ((StreamList) temp).getValue();
//            }
//            else {
//                assert temp instanceof StreamHLCollection;
//                this.isHL = true;
//                this.streamList = ((StreamHLCollection) temp).getValue();
//            }
//        }
//        else {
//            ExecutionTableEntry temp = getTableEntryWithLocal(listVar, evt, localEvt);
//            if (temp instanceof MaterializedList) {
//                this.materializedValue = (List<List<Object>>) temp.getValue();
//            }
//            else {
//                assert temp instanceof MaterializedHLCollection;
//                List<ExecutionTableEntry> TEs = ((MaterializedHLCollection) temp).getValue();
//                if (TEs.size() > 0) {
//                    ExecutionTableEntry first = TEs.get(0);
//                    assert first instanceof MaterializedList;
//                    for (ExecutionTableEntry TE : TEs) {
//                        this.materializedValue.add((List<Object>) TE.getValue());
//                    }
//                }
//            }
//        }
//    }
//
//
//    // the input can be a list of lists of values or a list of MaterializedList
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        System.out.println("The list size before union is " + this.materializedValue.size());
////        try {
////            for (int i=0; i<this.materializedValue.size(); i++) {
////                FileWriter writer = new FileWriter("output" + i + ".txt");
////                List<Object> t = this.materializedValue.get(i);
////                for(Object str: t) {
////                    writer.write((String) str + System.lineSeparator());
////                }
////                writer.close();
////            }
////        }
////        catch (Exception e) {
////                e.printStackTrace();
////        }
//        List<Object> value = this.materializedValue.stream().reduce((i1, i2) -> {
//            System.out.println(i2.size());
//            Set<Object> i3 = new HashSet<>();
//            i3.addAll(i1);
//            i3.addAll(i2);
//            return new ArrayList<>(i3);}).get();
//        System.out.println("The list size after union is : " + value.size());
//        return new MaterializedList(value);
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized executeStreamInput() {
//        List<Object> value;
//        // a publisher of Materialized List
//        if (isHL) {
//            // each element has to be materialized
//            Stream<List<Object>> x = this.streamList.map(i -> ((MaterializedList) i).getValue());
//            value = x.reduce((i1, i2) -> {
//                Set<Object> i3 = new HashSet<>();
//                i3.addAll(i1);
//                i3.addAll(i2);
//                return new ArrayList<Object>(i3);
//            }).get();
//        }
//        else {
//            Stream<List<Object>> x = this.streamList.map(i -> (List<Object>) i);
//             value = x.reduce((i1, i2) -> {
//                Set<Object> i3 = new HashSet<>();
//                i3.addAll(i1);
//                i3.addAll(i2);
//                return new ArrayList<Object>(i3);
//            }).get();
//        }
//        System.out.println("The list size after union is : " + value.size());
//        return new MaterializedList(value);
//    }
//
////    public void setHL(boolean HL) {
////        isHL = HL;
////    }
//
//    // todo: may change this
//    public void setMaterializedList(List<MaterializedList> materializedList) {
////        assert materializedList.size() > 0;
//        for (MaterializedList i : materializedList) {
//            this.materializedValue.add((List<Object>) i.getValue());
//        }
////        else {
////            for (List i : materializedList) {
////                List<Object> value = (List<Object>) i;
////                this.materializedValue.add(value);
////            }
////        }
//    }
//}

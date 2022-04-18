package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

// there are two union lists executors. one for Object, another for HL
public class ExeUnionList extends AwesomeStreamInputRunnable<List<Object>, List<Object>> {

    public ExeUnionList(List<List<Object>> input) {
        super(input);
    }

    public ExeUnionList(Stream<List<Object>> input) {
        super(input);
    }


    @Override
    public void executeBlocking() {
        List<Object> value = getMaterializedInput().stream().reduce((i1, i2) -> {
            System.out.println(i2.size());
            Set<Object> i3 = new HashSet<>();
            i3.addAll(i1);
            i3.addAll(i2);
            return new ArrayList<>(i3);}).get();
        System.out.println("The list size after union is : " + value.size());
        setMaterializedOutput(value);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }

    @Override
    public void executeStreamInput() {
        Stream<List<Object>> x = getStreamInput().map(i -> (List<Object>) i);
        List<Object> value = x.reduce((i1, i2) -> {
            Set<Object> i3 = new HashSet<>();
            i3.addAll(i1);
            i3.addAll(i2);
            return new ArrayList<>(i3);
        }).get();
        setMaterializedOutput(value);
    }


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

}

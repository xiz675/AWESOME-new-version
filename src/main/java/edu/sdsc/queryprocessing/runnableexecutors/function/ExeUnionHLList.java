package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class ExeUnionHLList extends AwesomeStreamInputRunnable<MaterializedList, List<Object>> {

        public ExeUnionHLList(List<MaterializedList> input) {
            super(input);
        }

        public ExeUnionHLList(Stream<MaterializedList> input) {
            super(input);
        }


        @Override
        public void executeBlocking() {
            List<List<Object>> tempValue = new ArrayList<>();
            for (MaterializedList i : getMaterializedInput()) {
                tempValue.add((List<Object>) i.getValue());
            }
            List<Object> value = tempValue.stream().reduce((i1, i2) -> {
                System.out.println(i2.size());
                Set<Object> i3 = new HashSet<>();
                i3.addAll(i1);
                i3.addAll(i2);
                return new ArrayList<>(i3);
            }).get();
            System.out.println("The list size after union is : " + value.size());
            setMaterializedOutput(value);
        }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedList(this.getMaterializedOutput());
    }

    @Override
        public void executeStreamInput() {
            Stream<List<Object>> x = getStreamInput().map(i -> ((MaterializedList) i).getValue());
            List<Object> value = x.reduce((i1, i2) -> {
                Set<Object> i3 = new HashSet<>();
                i3.addAll(i1);
                i3.addAll(i2);
                return new ArrayList<>(i3);
            }).get();
            setMaterializedOutput(value);
        }
    }
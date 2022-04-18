package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamList;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.SQLiteQueryUtil.getSQLResult;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.isInLocal;

public class GetColumn extends AwesomePipelineRunnable<AwesomeRecord, Object> {
//    private Publisher<AwesomeRecord> relation;
    private String cName;
    // there are actually only two types of input. One is a SQLite relation and the other one is Publisher
    // and the name can be provided or from the operator
    // change the ColumnToList type to getColumn
    // block
    public GetColumn(List<AwesomeRecord> input, String colName, boolean materializeOut) {
        super(input, materializeOut);
        this.cName = colName;
    }

    // streamOut
    public GetColumn(List<AwesomeRecord> input, String colName) {
        super(input);
        this.cName = colName;
    }

    // streamIn
    public GetColumn(Stream<AwesomeRecord> input, String colName) {
        super(input);
        this.cName = colName;
    }

    // pipeline
    public GetColumn(Stream<AwesomeRecord> input, String colName, boolean streamOut) {
        super(input, streamOut);
        this.cName = colName;
    }


//    if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//        this.rName = ope.getrName();
//        MaterializedRelation tempValue = (MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//        this.materializedValue = tempValue.getValue();
//    }
//        else {
//        this.streamInput = (StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//    }

//    public GetColumn(JsonObject config, String rName, String cName, VariableTable vte) {
//
//    }

    // for sub-operator, the output name is provided instaed of getting from the operator
    // since the relation will be provided as input when calling the streaming executors, so there is no need for this constructor
//    public GetColumnPhysical(JsonObject config,  Publisher<AwesomeRecord> relation, String cName, VariableTable vte) {
//        this.cName = cName;
//        this.relation = relation;
//        this.con = ParserUtil.sqliteConnect(config);
//    }




//
//    @Override
//    public MaterializedList<Object> execute() {
////        StreamList<Object> result = executeStreamOutput();
//        List<Object> t = this.materializedValue.stream().map(i -> i.getColumn(cName)).collect(Collectors.toList());
//        for (Object i : t) {
//            System.out.println((String) i);
//        }
//        return new MaterializedList<>(t);
////        return new MaterializedList<>(Flowable.fromPublisher(result.getValue()).toList().blockingGet());
//    }

    @Override
    public void executeStreamInput() {
        setMaterializedOutput(getStreamInput().map(i -> i.getColumn(cName)).collect(Collectors.toList()));
    }


    // needs to run a sql query on sqlite to get the results
    @Override
    public void executeStreamOutput() {
        setStreamOutput(getMaterializedInput().stream().map(i -> i.getColumn(cName)));
    }

    @Override
    public void executePipeline() {
        setStreamOutput(getStreamInput().map(i -> i.getColumn(cName)));
    }


    @Override
    public void executeBlocking() {
        setMaterializedOutput(getMaterializedInput().stream().map(i -> i.getColumn(cName)).collect(Collectors.toList()));
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
}
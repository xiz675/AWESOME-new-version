package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.VoidTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import edu.sdsc.utils.Pair;
import org.apache.commons.math3.random.HaltonSequenceGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.sdsc.utils.RelationUtil.*;

public class ExeStore extends AwesomeStreamInputRunnable<AwesomeRecord, String> {
    private String tbName;
    private Connection toCon;

//    // for parallel
//    public ExeStore(PipelineMode mode, Connection toCon, String tbName) {
//        super(mode);
//        this.toCon = toCon;
//        this.tbName = tbName;
//    }

    public ExeStore(Stream<AwesomeRecord> input, Connection toCon, String tbName) {
        super(input);
        this.toCon = toCon;
        this.tbName = tbName;
    }

    public ExeStore(List<AwesomeRecord> input, Connection toCon, String tbName) {
        super(input);
        this.toCon = toCon;
        this.tbName = tbName;
    }

    @Override
    public void executeBlocking() {
        List<AwesomeRecord> input = getMaterializedInput();
        assert input.size() > 0;
        AwesomeRecord r = input.get(0);
        Set<String> colNames = r.getColumnNames();
        List<String> col = new ArrayList<>(colNames);
        dropTable(toCon, tbName);
        createTable(toCon, tbName, col);
        List<AwesomeRecord> newInput = new ArrayList<>();
        if (tbName.equals("G")) {
            Map<Pair<String, String>, Integer> values = new HashMap<>();
            for (AwesomeRecord i : input) {
                Pair<Pair<String, String>, Integer> v =  i.getValue();
                if (values.containsKey(v.first)) {
                    values.put(v.first, v.second+values.get(v.first));
                }
                else {
                    values.put(v.first, v.second);
                }
            }
            for (Pair<String, String> crtWords : values.keySet()) {
                Pair<Pair<String, String>, Integer> value = new Pair<>(crtWords, values.get(crtWords));
                newInput.add(new AwesomeRecord(value));
            }
        }
        System.out.println("relation size:" + input.size());
        try {
            if (tbName.equals("G")) {
                System.out.println("relation size:" + newInput.size());
                insertAwesomRecordToTable(newInput, toCon, tbName, col);
            }
            else {
                insertAwesomRecordToTable(input, toCon, tbName, col);
            }
            toCon.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new VoidTableEntry();
    }

    @Override
    public void executeStreamInput() {
        Stream<AwesomeRecord> streamInput = getStreamInput();
        AwesomeRecord r = streamInput.findFirst().get();
        Set<String> colNames = r.getColumnNames();
        List<String> col = new ArrayList<>(colNames);
        dropTable(toCon, tbName);
        createTable(toCon, tbName, col);
        try {
            insertStreamToTable(streamInput, toCon, tbName, col, false, false);
            toCon.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }



}

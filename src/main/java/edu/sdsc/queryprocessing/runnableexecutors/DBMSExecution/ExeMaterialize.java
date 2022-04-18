package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.VoidTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;

import javax.json.Json;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static edu.sdsc.utils.RelationUtil.createTable;
import static edu.sdsc.utils.RelationUtil.insertAwesomRecordToTable;

public class ExeMaterialize extends AwesomeStreamInputRunnable<AwesomeRecord, String> {
    private String tbName;
    private Connection toCon;

    // for parallel
    public ExeMaterialize(PipelineMode mode, Json config, String dbName, String tbName) {
        super(mode);
//        this.toCon = toCon;
        this.tbName = tbName;
    }

    @Override
    public void executeBlocking() {
        List<AwesomeRecord> input = getMaterializedInput();
        assert input.size() > 0;
        AwesomeRecord r = input.get(0);
        Set<String> colNames = r.getColumnNames();
        List<String> col = new ArrayList<>(colNames);
        createTable(toCon, tbName, col);
        System.out.println("relation size:" + input.size());
        try {
            insertAwesomRecordToTable(input, toCon, tbName, col);
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

    }

}

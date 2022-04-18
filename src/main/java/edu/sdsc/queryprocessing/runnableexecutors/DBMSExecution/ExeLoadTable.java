package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.VoidTableEntry;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.LoadTable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.utils.RDBMSUtils;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;

import static edu.sdsc.utils.SQLExecuteUtil.excuteStore;

// load a DB table to SQLite
public class ExeLoadTable extends AwesomeBlockRunnable<LoadTable, String> {
    private Connection toCon;
    private RDBMSUtils db_util;
    private String tableName;

    public ExeLoadTable(LoadTable input, JsonObject config, Connection con) {
        super(input);
        String dbName = input.getSourceDatabase();
        this.tableName = input.getSourceTable();
        this.db_util = new RDBMSUtils(config, dbName);
        this.toCon = con;
    }

    @Override
    public void executeBlocking() {
        String stat = String.format("select * from %s", tableName);
        try {
            excuteStore(stat, tableName, db_util.getConnection(), toCon, true);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        db_util.closeConnection();
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new VoidTableEntry();
    }
}

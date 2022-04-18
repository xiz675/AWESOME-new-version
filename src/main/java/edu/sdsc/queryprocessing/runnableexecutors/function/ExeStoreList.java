package edu.sdsc.queryprocessing.runnableexecutors.function;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.VoidTableEntry;
import edu.sdsc.queryprocessing.planner.physicalplan.element.storage.StoreList;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import edu.sdsc.utils.RDBMSUtils;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;
import static edu.sdsc.utils.RelationUtil.*;

// make two executors for this
public class ExeStoreList<T> extends AwesomeStreamInputRunnable<T, String> {
    private boolean isHL = false;
    private boolean isNested = false;
    private String tableName;
    private boolean index;
    private List<String> columnNames;
    private Connection toCon;
    private RDBMSUtils rDBMS;

    // this can be list of object or list of table entry
    public ExeStoreList(JsonObject config, StoreList store, Connection sqlCon, List<T> input, boolean isHL, boolean isNested) {
        super(input);
        String dbName = store.getDbName();
        if (dbName.equals("*")) {
            this.toCon = sqlCon;
        }
        else {
            RDBMSUtils db_util = new RDBMSUtils(config, dbName);
            this.toCon = db_util.getConnection();
        }
        this.columnNames = store.getColumnNames();
        this.index = store.isIndex();
        this.tableName = store.getTableName();
        this.rDBMS = new RDBMSUtils(config, dbName);
        this.isNested = isNested;
    }

    public ExeStoreList(JsonObject config, StoreList store,  Connection sqlCon, Stream<T> input, boolean isHL) {
        super(input);
        String dbName = store.getDbName();
        if (dbName.equals("*")) {
            this.toCon = sqlCon;
        }
        else {
            RDBMSUtils db_util = new RDBMSUtils(config, dbName);
            this.toCon = db_util.getConnection();
        }
        this.columnNames = store.getColumnNames();
        this.index = store.isIndex();
        this.tableName = store.getTableName();
        this.rDBMS = new RDBMSUtils(config, dbName);
        this.isHL = isHL;
    }


    @Override
    // it can be a stream of HL or a stream of primitive which includes lists and scalar types, since it is a stream, can't know
    // if the inside is a list or not at this stage
    public void executeStreamInput() {
        try {
            if (rDBMS.isTableInDB(tableName)) {
                rDBMS.deleteTable(tableName);
            }
            createTable(toCon, tableName, columnNames);
            insertStreamToTable(getStreamInput(), toCon, tableName, columnNames, index, isHL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeBlocking() {
        try {
            if (rDBMS.isTableInDB(tableName)) {
                rDBMS.deleteTable(tableName);
            }
            createTable(toCon, tableName, columnNames);
            insertListToTable(getMaterializedInput(), toCon, tableName, columnNames, index, isNested);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new VoidTableEntry();
    }
}


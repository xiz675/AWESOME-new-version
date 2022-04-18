package edu.sdsc.queryprocessing.scheduler.utils.metadata;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;

public class ExecuteSQLMeta extends ExecutorMetaData {
    private String sql;
    private String polystore;
    private String dbName;

    public ExecuteSQLMeta(PipelineMode cap, PipelineMode mode, String sql, String polystore, String dbName) {
        super(ExecutorEnum.ExecuteSQL, cap, mode);
        this.sql = sql;
        this.polystore = polystore;
        this.dbName = dbName;
    }

    public String getSql() {
        return sql;
    }

    public String getDbName() {
        return dbName;
    }

    public String getPolystore() {
        return polystore;
    }
}

package edu.sdsc.queryprocessing.executor.utils;

import edu.sdsc.datatype.execution.AwesomeRecord;
import io.reactivex.rxjava3.core.Flowable;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.util.Set;

public class SQLiteQueryUtil {

    public static ResultQuery<Record> getSQLResult(Connection sqlCon, String relationName, String colNames) {
//        Connection conn = db_util.getConnection();
        DSLContext create = DSL.using(sqlCon, SQLDialect.SQLITE);
        String sql = String.format("select %s from %s", colNames, relationName);
        return create.resultQuery(sql);
    }


}

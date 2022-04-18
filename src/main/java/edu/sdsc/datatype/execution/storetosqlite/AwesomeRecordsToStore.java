package edu.sdsc.datatype.execution.storetosqlite;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.utils.Pair;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static edu.sdsc.utils.RelationUtil.*;

public class AwesomeRecordsToStore extends ResultToStore{
    private List<AwesomeRecord> result;

    public AwesomeRecordsToStore(List<AwesomeRecord> result, List<String> cols, String ouptStr) {
        super(cols, ouptStr);
        this.result = result;
    }

    public List<AwesomeRecord> getResult() {
        return result;
    }

    @Override
    public void storeToSQLite(Connection toCon) throws SQLException {
        createTable(toCon, this.getOuptStr(), this.getCols());
        System.out.println("relation size:" + this.result.size());
        insertAwesomRecordToTable(this.getResult(), toCon, this.getOuptStr(), this.getCols());
    }

    @Override
    public Pair<String, List<String>> getValue() {
        return null;
    }

    @Override
    public void setValue(Pair<String, List<String>> value) {

    }

    @Override
    public String toSQL() {
        return null;
    }

    @Override
    public String toCypher() {
        return null;
    }
}

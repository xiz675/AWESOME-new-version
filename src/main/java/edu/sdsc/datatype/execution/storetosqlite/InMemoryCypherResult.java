package edu.sdsc.datatype.execution.storetosqlite;

import edu.sdsc.utils.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static edu.sdsc.utils.TinkerpopUtil.storeInMeomoryCypherResult;

public class InMemoryCypherResult extends ResultToStore {
    private List<Map<String, Object>> result;
    // for cypher query result from Tinkerpop
    public InMemoryCypherResult(List<Map<String, Object>> result, List<String> cols, String ouptStr) {
        super(cols, ouptStr);
        this.result = result;
    }

    public List<Map<String, Object>> getResult() {
        return result;
    }

    @Override
    public void storeToSQLite(Connection toCon) throws SQLException {
        System.out.println("relation size:" + this.result.size());
        storeInMeomoryCypherResult(this.result, this.getCols(), this.getOuptStr(), toCon);
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

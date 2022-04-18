package edu.sdsc.datatype.execution.storetosqlite;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.utils.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public abstract class ResultToStore implements ExecutionTableEntry<Pair<String, List<String>>> {
    private List<String> cols;
    private String ouptStr;


    ResultToStore(List<String> cols, String ouptStr) {
        this.cols = cols;
        this.ouptStr = ouptStr;
        setValue(new Pair<>(ouptStr, cols));
    }

    public abstract void storeToSQLite(Connection toCon) throws SQLException;

    public List<String> getCols() {
        return cols;
    }

    public String getOuptStr() {
        return ouptStr;
    }
}

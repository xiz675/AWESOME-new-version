package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MaterializedList<T> extends ExecutionTableEntryMaterialized<List<T>> {

    public MaterializedList(List<T> value) {
        this.setValue(value);
    }

    @Override
    public List<T> getValue() {
        return super.getValue();
    }

    @Override
    public String toSQL() {
        if (this.getValue() == null) {
            return " ";
        }
        List<T> v = this.getValue();
    //        System.out.println(v);
    //        assert v.size() > 0;
        String valueStr = null;
        if (v.size() == 0) {
            return " ";
        }
        System.out.println("list variable of a sql/cypher query has size: " + v.size());
        if (v.get(0) instanceof Integer) {
            valueStr = v.stream().map(Object::toString)
                    .collect(Collectors.joining(", "));
        }
        else if (v.get(0) instanceof String) {
            valueStr = "'" + String.join("', '", ((ArrayList<String>) v)) + "'";

        }
        return valueStr;
    }

    @Override
    public String toCypher() {
        String temp = this.toSQL();
        return "[" + temp + "]";
    }

    @Override
    public String toSolr() {
        if (this.getValue() == null) {
            return "[ ]";
        }
        List<T> v = this.getValue();
        //        System.out.println(v);
        //        assert v.size() > 0;
        String valueStr = null;
        if (v.size() == 0) {
            return "[]";
        }
        System.out.println("list variable of a sql/cypher query has size: " + v.size());
        if (v.get(0) instanceof Integer) {
            valueStr = v.stream().map(Object::toString)
                    .collect(Collectors.joining(", "));
        }
        else if (v.get(0) instanceof String) {
            valueStr = "'" + String.join("', '", ((ArrayList<String>) v)) + "'";

        }
        return "[" + valueStr + "]";
    }
}

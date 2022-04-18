package edu.sdsc.datatype.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Graphdata {
    public String source;
    public String target = null;
    public Integer edgeWeight = 0;
    public boolean hasEdge = false;
    public Graphdata(String source) {
        this.source = source;
    }
    public Graphdata(String source, String target) {
        this.source = source;
        this.target = target;
        this.hasEdge = true;
        this.edgeWeight = 1;
    }

    public Graphdata(String source, String target, Integer count) {
        this.source = source;
        this.target = target;
        this.hasEdge = true;
        this.edgeWeight = count;
    }

    public List<String> getNodes() {
        List result = new ArrayList();
        result.add(source);
        if (hasEdge) {
            result.add(target);
        }
        return result;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Graphdata graph = (Graphdata) o;
        return Objects.equals(source, graph.source) &&
                Objects.equals(target, graph.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target);
    }

    @Override
    public String toString() {
        return source+"-"+target+":"+edgeWeight.toString();
    }
}

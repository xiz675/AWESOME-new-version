package edu.sdsc.datatype.execution;

import java.util.*;

public class Node extends GraphElement {


    public Node(String label, Object... keyValue) {
        super(label, keyValue);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Node o= (Node) obj;
        return this.getLabel().equals(o.getLabel()) && this.getProperties().equals(o.getProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getLabel(), this.getProperties());
    }
}

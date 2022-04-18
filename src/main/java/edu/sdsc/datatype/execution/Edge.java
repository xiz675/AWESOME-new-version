package edu.sdsc.datatype.execution;
import java.util.Objects;

public class Edge extends GraphElement {
    private Node firstNode;
    private Node secondNode;
    private boolean hasDirection;


    public Edge(String label, Object... keyValue) {
        super(label, keyValue);
    }

    public void setNodes(Node firstNode, Node secondNode, boolean dir) {
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.hasDirection = dir;
    }

    public Node getFirstNode() {
        return firstNode;
    }

    public Node getSecondNode() {
        return secondNode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Edge o = (Edge) obj;
        return this.hasDirection == o.hasDirection &&
          this.firstNode.equals(o.firstNode) &&
          this.secondNode.equals(o.secondNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.firstNode, this.secondNode, this.hasDirection);
    }
}


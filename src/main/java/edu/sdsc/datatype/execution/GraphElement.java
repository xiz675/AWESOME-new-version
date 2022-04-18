package edu.sdsc.datatype.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GraphElement {
    private String label = null;
    private Map<String, Object> property = new HashMap<>();

    public GraphElement() {

    }

    public GraphElement(Object... keyValue) {
        for (int i=0; i < keyValue.length - 1; i=i+2) {
            this.property.put((String) keyValue[i], keyValue[i+1]);
        }
    }

    public GraphElement(String label, Object... keyValue) {
        this.label = label;
        for (int i=0; i < keyValue.length - 1; i=i+2) {
            this.property.put((String) keyValue[i], keyValue[i+1]);
        }
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void addProperty(String key, Object value) {
        this.property.put(key, value);
    }

    public Map<String, Object> getProperties() {
        return this.property;
    }

    public Object[] getPropertyArray() {
        int s = this.property.keySet().size();
        Iterator<String> pKey = this.property.keySet().iterator();
        Object[] property = new Object[2*s];
        for (int index=0; index < 2*s; index++) {
            String tempKey = pKey.next();
            property[index] = tempKey;
            property[index+1] = this.property.get(tempKey);
            index = index + 2;
        }
        return property;
    }

    public Object getProperty(String key) {
        return this.property.get(key);
    }

    public boolean hasProperty(String key) {
        return this.property.containsKey(key);
    }


    public String getLabel() {
        return this.label;
    }


    @Override
    public String toString() {
        if (this instanceof Edge) {
            return String.format("node1 %s node2 %s", ((Edge) this).getFirstNode().getProperties(),  ((Edge) this).getSecondNode().getProperties());
        }
        else {
            return label + property;
        }
    }


    public static void main(String[] args) {
        GraphElement x = new GraphElement("s", "k1", 1, "k2", 2);
        int count = (Integer) x.getProperty("k1");
        x.addProperty("k1", count+1);
        System.out.println(x.getProperties());
    }


}

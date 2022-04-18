package edu.sdsc.variables.logicalvariables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

// modify this to make list a collection type and can be a collection of other VairableTableEntry
public class ListTableEntry extends VariableTableEntry {
    private Integer size;
    private Integer elementType = 0;
    private Object metaData = 0;
    //private ArrayList<Object> value = null;
    public ListTableEntry(String name, Integer... block){
        super(name, DataTypeEnum.List.ordinal(), block);
    }
    public ListTableEntry(String name, Integer elementType, Integer size, Integer... block){
        super(name, DataTypeEnum.List.ordinal(), block);
        this.size = size;
        this.elementType = elementType;
    }

    public ListTableEntry(String name, Integer elementType, Integer size, ArrayList<Object> value, Integer... block){
        this(name,  elementType, size, block);
        setValue(value);
    }


    @Override
    public Map getPropertyMap(){
        Map propertyMap = new HashMap();
        propertyMap = super.getPropertyMap();
        propertyMap.put("elementType", elementType);
        propertyMap.put("size", size);
        return propertyMap;
    }
    public Object getElementValue(Integer idx) {
        if (this.getValue() == null) {
            return null;
        }
        else if (idx >= this.size) {
            throw new IndexOutOfBoundsException("the index is larger than list size");
        }
        else {
            return ((ArrayList<Object>) this.getValue()).get(idx);
        }
    }

    public Integer getSize() {
        return size;
    }

    public Integer getElementType() {
        return elementType;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setElementType(Integer elementType) {
        this.elementType = elementType;
    }

    public void setMetaData(Map<String, String> schema) {
        this.metaData = schema;
    }

    public void setMetaData(Object elementType) {
        this.metaData = elementType;
    }


    public Object getMetaData() {
        return metaData;
    }

    //    public void setValue(ArrayList<Object> value) {
//        this.value = value;
//    }
//
//    public ArrayList<Object> getValue() {
//        return value;
    //}
}

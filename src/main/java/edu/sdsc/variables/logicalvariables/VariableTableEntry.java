/*
 * Copyright (c) 2019.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *
 */

package edu.sdsc.variables.logicalvariables;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class VariableTableEntry {
    private String name; //name of the variable
    private Integer type;// type INT, LONG, Text etc
    //private String handleClass;
    private Boolean store = false;//
    //private Boolean importLib = false;// Import Library
    private Integer blockID = -1;
    private Integer load;
    private boolean dependent = true;
    private boolean partioned = false;
    private String originalNode; // Resolved Dependency if dependent = true
    private boolean indexed = false;
    private String indexType;
    private String indexName;
    //private boolean vector = false;
    //private Integer dimension = 0;
    //private Integer elementType;
    private boolean storeIndexed = false;
    private boolean awesomeIndex = false;
    private Map optimizationProperty;
    private String optimizationClass;
    private boolean ordered = false;
    //private List orderList;
    //private List partionList;
    //private Integer orderType;
    //private Integer groupType;
    private boolean storagePointer = false;
    private String storage;
    private Integer storageType;
    private Integer volume;
    private Date localdate;
    private long timeStamp;
    private Object value = null;
    private Integer parentID = -1;
    private boolean localVar = false;
    //private boolean table = false;
    //private TableInfo tableInfo;
    //private Integer partitionType;


    public void setLocalVar() {
        this.localVar = true;
    }

    public boolean isLocalVar() {
        return localVar;
    }

    public void setParentID(Integer parentID) {
        this.localVar = true;
        this.parentID = parentID;
    }

    public Integer getParentID() {
        return parentID;
    }

    public VariableTableEntry(String name, Integer type, Integer... blockID){
        this.name = name;
        this.type = type;
        if (blockID.length==1){
            this.blockID = blockID[0];
            this.setLocalVar();
        }
    }
    public Map getPropertyMap(){
        Map propertyMap = new HashMap();
        //Basic Properties of a Variable
        propertyMap.put("name",name);
        propertyMap.put("type",type);
        propertyMap.put("blockID",blockID);
        //propertyMap.put("handleClass",handleClass);
        //propertyMap.put("date",localdate);
        //propertyMap.put("timestamp",timeStamp);
        propertyMap.put("volume", volume);
        propertyMap.put("optimizationProp", optimizationProperty);
        propertyMap.put("optimizationClass", optimizationClass);
        propertyMap.put("store", store);
        propertyMap.put("cost", load);
        //propertyMap.put("elementType", elementType);
        //Optional Property of a variable default is false
        if(storagePointer){
            propertyMap.put("storage", storage);
            propertyMap.put("storageType", storageType);
        }
        if(indexed){
            propertyMap.put("indexName", indexName);
            propertyMap.put("awesomeIndex", awesomeIndex);
            propertyMap.put("storeIndex", awesomeIndex);
            propertyMap.put("storage", storage);
        }
        if(dependent){
            propertyMap.put("ancestors", originalNode);
        }
        //if(ordered){
        //    propertyMap.put("orderList",orderList);
        //    propertyMap.put("orderType",orderType);
        //}
//        if(partioned){
//            propertyMap.put("partitionType",partitionType);
//            propertyMap.put("orderType",orderType);
//        }
//        if(vector){
//
//            propertyMap.put("dimension",dimension);
//        }
//        if(table){
//            propertyMap.put("tableinfo",tableInfo);
//
//        }
    return propertyMap;
    }
    public String getName() {
        return name;
    }
    public void setBlockID(Integer blockID) {
        this.blockID = blockID;
    }
    public Integer getBlockId() {
        return blockID;
    }
    public void setName(String name) {
        this.name = name;
    }
    public boolean isStoragePointer() {
        return storagePointer;
    }
    public void setStoragePointer(boolean storagePointer) {
        this.storagePointer = storagePointer;
    }
    public Integer getStorageType() {
        return storageType;
    }
    public void setStorageType(Integer storageType) {
        this.storageType = storageType;
    }
    public Integer getVolume() {
        return volume;
    }
    public void setVolume(Integer volume) {
        this.volume = volume;
    }
    public boolean isOrdered() {
        return ordered;
    }
    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }
    public Integer getType() {
        return type;
    }
    public void setType(Integer type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    //    public String getHandleClass() {
//        return handleClass;
//    }
//
//    public void setHandleClass(String handleClass) {
//        this.handleClass = handleClass;
//    }

    public Boolean getStore() {
        return store;
    }

    public void setStore(Boolean store) {
        this.store = store;
    }

    public Integer getLoad() {
        return load;
    }

    public void setLoad(Integer load) {
        this.load = load;
    }

    public boolean isDependent() {
        return dependent;
    }

    public void setDependent(boolean dependent) {
        this.dependent = dependent;
    }

    public boolean isPartioned() {
        return partioned;
    }

    public void setPartioned(boolean parttioned) {
        this.partioned = parttioned;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public boolean isStoreIndexed() {
        return storeIndexed;
    }

    public void setStoreIndexed(boolean storeIndexed) {
        this.storeIndexed = storeIndexed;
    }

    public boolean isAwesomeIndex() {
        return awesomeIndex;
    }

    public void setAwesomeIndex(boolean awesomeIndex) {
        this.awesomeIndex = awesomeIndex;
    }

    public Map getOptimizationProperty() {
        return optimizationProperty;
    }

    public void setOptimizationProperty(Map optimizationProperty) {
        this.optimizationProperty = optimizationProperty;
    }

    public String getOptimizationClass() {
        return optimizationClass;
    }

    public void setOptimizationClass(String optimizationClass) {
        this.optimizationClass = optimizationClass;
    }

    public VariableTableEntry() {
        optimizationProperty = new HashMap();
        timeStamp = System.currentTimeMillis();

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());


        //method 2 - via Date
        localdate = new Date();
        //System.out.println(new Timestamp(localdate.getTime()));



    }

    //    public boolean isTable() {
//        return table;
//    }
//    public void setTable(boolean table) {
//        this.table = table;
//        if(table)
//        {
//            tableInfo = new TableInfo();
//        }
//
//    }
//    public TableInfo getTableInfo() {
//        return tableInfo;
//    }
//    public void setTableInfo(TableInfo tableInfo) {
//        this.tableInfo = tableInfo;
//    }










//    public List getOrderList() {
//        return orderList;
//    }
//    public void setOrderList(List orderList) {
//        this.orderList = orderList;
//    }
//    public List getPartionList() {
//        return partionList;
//    }
//    public void setPartionList(List partionList) {
//        this.partionList = partionList;
//    }
//
//    public Integer getOrderType() {
//        return orderType;
//    }
//
//    public void setOrderType(Integer orderType) {
//        this.orderType = orderType;
//    }
//
//    public Integer getGroupType() {
//        return groupType;
//    }
//
//    public void setGroupType(Integer groupType) {
//        this.groupType = groupType;
//    }
//
//    public Integer getDimension() {
//        return dimension;
//    }
//
//    public void setDimension(Integer dimension) {
//        this.dimension = dimension;
//    }
//    public void setElementType(Integer type){
//        this.elementType = type;
//
//    }

}

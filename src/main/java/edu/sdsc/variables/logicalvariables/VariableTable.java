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
import edu.sdsc.utils.Pair;

import java.util.*;

public class VariableTable {
    private Map<Integer, VariableTableEntry> variables;
    public VariableTable() {
       variables = new HashMap<>();
    }
    private Map<Pair<String, Integer>, Integer> nameToID = new HashMap<>();

    public Integer getSize() {
        return variables.size();
    }


    public void insertEntry(Integer id, VariableTableEntry tableEntry){
//        VariableTableEntry vte = new VariableTableEntry();
//        vte.setName(name);
        Integer blockId = tableEntry.getBlockId();
        String name = tableEntry.getName();
        //Pair key = new Pair(name, -1);
        Pair<String, Integer> key = new Pair<>(name, blockId);
        nameToID.put(key, id);
        variables.put(id, tableEntry);

    }

    public Map<Integer, VariableTableEntry> getVariables() {
        return variables;
    }

    public static VariableTableEntry createVariableEntry(String name, String type, Integer... block) throws RuntimeException {
        VariableTableEntry x;
        if (type.equals("Relation")){
            x = new RelationTableEntry(name, block);
        }
        else if (type.equals("List")){
            x = new ListTableEntry(name, block);
        }
        else if (type.equals("Matrix")){
            x = new MatrixTableEntry(name,  block);
        }
        else if(type.equals("Integer") || type.equals("String")){
            x = new LiteralTableEntry(name, DataTypeEnum.valueOf(type).ordinal(),  block);
        }
        else if(type.startsWith("TextMatrix")) {
            x = new TextMatrixTableEntry(name, type, block);
        }
        else if(type.equals("Undecided")){
            x = new UndecidedTableEntry(name, block);
        }
        else if(type.equals("PropertyGraph")) {
            x = new PGTableEntry(name, block);
        }
        else {
            throw new IllegalArgumentException("undefined type: "+type);
        }
        return x;
    }

    public void batchDeclare(Integer variableID, List<String> names, String type, Integer... block){
        String name;

        for (String name1 : names) {
            name = name1;
            VariableTableEntry vte = createVariableEntry(name, type, block);
            insertEntry(variableID, vte);
            variableID = variableID + 1;
        }

    }

//    public void batchInsertToTable(Integer variableID, List<String> names, List<String> types, Integer... block) throws IllegalAccessError{
//        String name;
//        String typeString;
//        if (names.size()!=types.size()){
//            throw new IllegalAccessError("number of variables is not equal to that of output variable of function");
//        }
//        for (int i = 0; i < names.size(); i++) {
//            name = names.get(i);
//            typeString = types.get(i);
//            VariableTableEntry vte = createVariableEntry(name, typeString, null, block);
//            insertEntry(variableID, vte);
//            variableID = variableID+1;
//            }
//
//    }

    public String getVarType(Integer vID){
        Integer type = variables.get(vID).getType();
        return DataTypeEnum.values()[type].name();
    }

    public Object getVarValue(Integer vID) {
        return variables.get(vID).getValue();
    }
    // todo: 1:50pm add to functions

    public Integer getParentID(Integer vID) {
        return variables.get(vID).getParentID();
    }

    public Integer getBlockID(Integer vID) {
        return variables.get(vID).getBlockId();
    }

    public boolean isLocal(Integer vID) {
        return variables.get(vID).isLocalVar();
    }

    public String getVarName(Integer vID) {
        return variables.get(vID).getName();
    }
    public VariableTableEntry getVarTableEntryForComposite(Integer vID, String name, String idx1, String idx2, Integer... block) throws IllegalArgumentException {
        VariableTableEntry vte = variables.get(vID);
        String typeString = getVarType(vID);
        if (idx1 == null && idx2 == null) {
            vte.setName(name);
            return vte;
        }
        else if (idx1 != null && idx2 == null && typeString.equals("List")) {
            ListTableEntry lte = (ListTableEntry) vte;
            String elementType = DataTypeEnum.values()[lte.getElementType()].name();
            Object value = lte.getElementValue(Integer.parseInt(idx1));
            if (elementType.equals("Integer") || elementType.equals("String")) {
                return new LiteralTableEntry(name, DataTypeEnum.valueOf(elementType).ordinal(), value, block);
            }
            else {
                return createVariableEntry(name, elementType, block);
            }
        }
        else if (idx1 != null && idx2 != null && typeString.equals("Matrix")) {
            MatrixTableEntry lte = (MatrixTableEntry) vte;
            double value = lte.getElement(Integer.parseInt(idx1), Integer.parseInt(idx2));
            return new LiteralTableEntry(name, DataTypeEnum.valueOf("Integer").ordinal(), value, block);
        }
        else{throw new IllegalArgumentException("the input index is in wrong format");}
    }





//    public void addDimention(String name, Integer dimention){
//        variables.get(name).setDimension(dimention);
//    }
//    public void addElementType(String name, Integer type){
//        variables.get(name).setElementType(type);
//    }
//
//    public void setGroup(List<String> groups){
//        for(String group : groups){
//            variables.get(group).setOrdered(true);
//            variables.get(group).setOrderList(groups);
//        }
//    }
//    public void setOrder(List<String> groups) {
//        for (String group : groups) {
//            variables.get(group).setOrdered(true);
//            variables.get(group).setOrderList(groups);
//        }
//    }

    public VariableTableEntry getVariableProperties(Integer id){
        return variables.get(id);

    }
    public boolean isInVariableTable(String name, Integer... blockID){
        System.out.println(name);
        Pair<String, Integer> temp;
        Pair<String, Integer> temp2;
        if (blockID.length == 1){
            temp = new Pair<>(name, blockID[0]);
            temp2 = new Pair<>(name, -1);
            if (nameToID.containsKey(temp) || nameToID.containsKey(temp2)){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            temp = new Pair<>(name, -1);
            return nameToID.containsKey(temp);
        }
    }


    public Integer searchVariableInBlock(String name, Integer blockId){
        Pair<String, Integer> temp = new Pair<>(name, blockId);
        assert nameToID.containsKey(temp);
        return nameToID.get(temp);
    }


    public Pair<Integer, Integer> searchVariable(String name, Integer... blockId){
        Integer id;
        Pair<String, Integer> temp;
        if (blockId.length == 1){
            temp = new Pair<>(name, blockId[0]);
            if (nameToID.containsKey(temp)){
                return new Pair<>(nameToID.get(temp), blockId[0]);
            }
            else{
                temp = new Pair<>(name, -1);
                id = nameToID.get(temp);
                if (id == null) {
                    throw new IllegalArgumentException("no variable: " + name);
                }
                return new Pair<>(id, -1);
            }
        }
        else{
            id = nameToID.get(new Pair<>(name, -1));
            if (id != null) {
                return new Pair<>(id, -1);
            }
            else {
                throw new IllegalArgumentException("no variable: " + name);
            }
        }
    }

    public void variableStaticResolve(){

    }

}

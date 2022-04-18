package edu.sdsc.utils;

import edu.sdsc.datatype.parser.AwsmDataType;
import edu.sdsc.datatype.parser.*;
import edu.sdsc.datatype.parser.FuncInput;
import edu.sdsc.variables.logicalvariables.RelationTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTable;


import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class FunctionUtil {
    public static Pair<List<FuncInput>, List<AwsmDataType>> validateFunction(String name, List<FuncInput> parameter, List<String> names, VariableTable vte) throws SQLException {
        List<FuncInput> inputInformation = getVariableInformation(parameter, vte);
//        for (FuncInput f: inputInformation) {
//            System.out.println(f.name + ":" + f.type);
//        }
        String query = "Select * from functionsignaturetable WHERE name = \""+name+"\"";
        //System.out.println(query);
        Connection con = ParserUtil.dbConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        List<AwsmDataType> outputType;
        if (!rs.next()) {throw new IllegalArgumentException("no function:" + name);}
        else{
            do {
                String input = rs.getString("inputtype");
                String optional = rs.getString("optional");
                if (checkInput(input, optional, inputInformation)){
                    String output = rs.getString("output");
                    outputType = parseOutput(output, names);
                    return new Pair<>(inputInformation, outputType);
                }
            } while (rs.next());
            throw new IllegalArgumentException("the input of function: "+name+" is wrong");
        }
    }
    // todo: add more type
    private static List<AwsmDataType> parseOutput(String output, List<String> names){
        List<String> ouptTypes = Arrays.asList(output.split("#"));
        List<AwsmDataType> result = new ArrayList<>();
        String name = "~";
        boolean testName = (names.size() != 1) || (!names.get(0).equals("_"));
        if (testName && ouptTypes.size() != names.size()){
            throw new IllegalAccessError("number of variables is not equal to that of output variable of function");
        }
        for (int i = 0; i < ouptTypes.size(); i++) {
            String[] types = ouptTypes.get(i).split("<");
            String mainType = types[0];
            if (testName) {
                name = names.get(i);
            }
//            if (name.equals("~")){
//                continue;
//            }
            AwsmDataType variable;
            switch (mainType) {
                case "Integer":
                case "String":
                case "Double":{
                    variable = new AwsmLiteral(name, mainType);
                    break;
                }
                case "Matrix":
                    variable = new AwsmMatrix(name);
                    break;
                case "Boolean":
                    variable = new AwsmBoolean(name);
                    break;
                case "List": {
                    AwsmList tempVariable = new AwsmList();
                    tempVariable.name = name;
                    if (types.length > 1) {
                        tempVariable.elmtType = types[1].split(">")[0];
                    }
                    variable = tempVariable;
                    break;
                }
                case "PropertyGraph":
                    variable = new AwsmPropertyGraph(name);
                    break;
                case "TextMatrix":
                    variable = new AwsmTextMatrix(name);
                    break;
                default: {
                    assert mainType.equals("Relation");
                    AwsmRelation tempVariable = new AwsmRelation();
                    tempVariable.name = name;
                    if (types.length > 1) {
                        String schema = types[1].split(">")[0];
                        Map<String, String> schemaMap = new HashMap<>();
                        for (String sch : schema.split(",")) {
                            schemaMap.put(sch.split(":")[0], sch.split(":")[1]);
                        }
                        tempVariable.schema = schemaMap;
                    }
                    variable = tempVariable;
                    break;
                }
            }
            result.add(variable);
        }
        return result;

    }


    private static List<FuncInput> getVariableInformation(List<FuncInput> input, VariableTable vte){
        List<FuncInput> result = new ArrayList<>();
        for (FuncInput f: input){
            if (!f.variable){
                result.add(f);
            }
            else{
                result.add(getSingleVariableInfo(f,vte));
            }
        }
        return result;
    }

    private static FuncInput getSingleVariableInfo(FuncInput f, VariableTable vte) {
        String name = f.name;
        Integer bID;
        Pair<Integer, Integer> vIDbID;
        if (name.contains(".")) {
            String tName = name.split("\\.")[0];
            String col = name.split("\\.")[1];
            RelationTableEntry rte = SQLParseUtil.checkSystemTable(vte, tName, f.block);
            if (rte.isLocalVar() || rte.getColumnType(col) != null) {
                f.type = "Column";
                vIDbID = vte.searchVariable(tName, f.block);
                f.varID = vIDbID.first;
                bID = vIDbID.second;
                f.block = bID;
                if (bID != -1) {
                    f.localVar = true;
                    f.parentVarID = vte.getParentID(f.varID);
                }
            }
            else {throw new IllegalArgumentException("no column:" + name);}
        }
        else {
            vIDbID = vte.searchVariable(name, f.block);
            f.varID = vIDbID.first;
            f.type = vte.getVarType(f.varID);
            bID = vIDbID.second;
            f.block = bID;
            if (bID != -1) {
                f.localVar = true;
                f.parentVarID = vte.getParentID(f.varID);
            }
            Object value = vte.getVarValue(f.varID);
            String type = vte.getVarType(f.varID);
            if (value!=null) {
                // if v alue is not null, then set it as an non-variable
                f.setValue(value, vte.getVarType(f.varID));
//                f.value =  value;
            }
        }
        return f;
    }

    private static boolean checkInput(String input, String optional, List<FuncInput> parameter){
        String[] inputList = input.split(",");
        Map<String, String> optionalMap = new HashMap<>();
        if (optional != null && optional.length()>0) {
            String[] optionalList = optional.split(",");
            for (String s: optionalList) {
                optionalMap.put(s.split(":")[0], s.split(":")[1]);
            }
        }
        int i;
//        for (FuncInput x: parameter){
//            System.out.println("type is " + x.type);
//            System.out.println(x.key);
//        }
        for (i=0; i < parameter.size(); i++){
            FuncInput f = parameter.get(i);
            if (f.key != null){break;}
            else if (!f.type.equals("Undecided") && !f.type.equals(inputList[i])){
                return false;
            }
        }
        for (int j = i; j<parameter.size(); j++){
            FuncInput f = parameter.get(i);
            String key = f.key;
            if ((!optionalMap.containsKey(key))||(!f.type.equals(optionalMap.get(key)))){
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws SQLException{
        List<FuncInput> input = new ArrayList<>();
        FuncInput a = new FuncInput();
        FuncInput b = new FuncInput();
        VariableTable x = new VariableTable();
        Map<String, String> schema = new HashMap<>();
        schema.put("text", "varchar");
        schema.put("docID", "Integer");
        RelationTableEntry rte = new RelationTableEntry("test",schema);
        int variableID = 0;
        x.insertEntry(variableID, rte);
        a.variable = true;
        a.name = "test.text";
        input.add(a);
        b.variable = true;
        b.name = "test.docID";
        b.key = "docID";
        input.add(b);
        ArrayList<String> output = new ArrayList<>();
        output.add("x");
        Pair<List<FuncInput>, List<AwsmDataType>> allInfo = validateFunction("sentenceTokenizer", input, output, x);
        List<AwsmDataType> ot = allInfo.second;
        // add input to the rhs of output json
        // insert output to variable table and add to the lhs
        for (AwsmDataType d : ot) {
            x.insertEntry(variableID, d.toTableEntry());
//            System.out.println(d.toJsonObject(variableID));
            variableID += 1;
        }
        JsonArray d = Json.createArrayBuilder().build();



    }

}

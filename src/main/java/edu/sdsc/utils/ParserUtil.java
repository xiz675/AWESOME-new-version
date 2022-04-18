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

package edu.sdsc.utils;

import edu.sdsc.variables.logicalvariables.PGTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ParserUtil {

    static final Logger logvalue = LoggerFactory.getLogger(ParserUtil.class);

    static boolean validateTuple(String s, String fieldName, String tableName) throws SQLException {

        Connection con = ParserUtil.dbConnection();
        int count = 0;

        boolean flag = false;
        String query = "Select count(*) from " + tableName + " where " + fieldName + " = " + s;
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        while (rs.next()) {
            count = rs.getInt("count");
            if (count > 0) {
                flag = true;
            }
        }
        return flag;
    }

    public static double[] jsonArrayToDoubleArray(JsonArray value){
        double[] rsult = new double[value.size()];
        for (int i = 0; i < value.size(); i++){
            rsult[i] = (double) value.getInt(i);
        }
        return rsult;
    }

    public static ArrayList<Object> jsonArrayToList(JsonArray value, String type){
        ArrayList<Object> rsult = new ArrayList<>();
        if (type.equals("Integer")){
            for (int i = 0; i < value.size(); i++){
                rsult.add(value.getInt(i));
            }
        }
        else if(type.equals("String")){
            for (int i = 0; i < value.size(); i++){
                rsult.add(value.getString(i));
            }
        }
        return rsult;
    }

    public static List<Object> jsonArrayToListWithDifferentEleType(JsonArray value, List<String> types){
        List<Object> rsult = new ArrayList<>();
        assert value.size() == types.size();
        String type;
        for (int i = 0; i < value.size(); i++) {
            type = types.get(i);
            if (type.equals("Integer")){
                rsult.add(value.getInt(i));
            }
            else if(type.equals("String")){
                rsult.add(value.getString(i));
            }

        }
        return rsult;
    }



    public static ArrayList<String> jsonArrayToStringList(JsonArray value){
        ArrayList<String> rsult = new ArrayList<>();
        for (int i = 0; i < value.size(); i++){
                rsult.add(value.getString(i));
        }
        return rsult;
    }



    public static double[][] jsonArrayToMatrix(JsonArray value){
        double[][] rsult = new double[value.size()][value.getJsonArray(0).size()];
        for (int i = 0; i < value.size(); i++){
            double[] temp= jsonArrayToDoubleArray(value.getJsonArray(i));
            rsult[i] = temp;
        }
        return rsult;
    }


    public static JsonArray extractKey(JsonArray ary, String key){
        JsonArrayBuilder rslt = Json.createArrayBuilder();
        JsonString temp;
        for (int i = 0; i < ary.size(); i++) {
            temp = ary.getJsonObject(i).getJsonString(key);
            if(temp!=null){
                rslt.add(temp);
            }
        }
        return rslt.build();
    }

    public static JsonArray extractObject(JsonArray ary, String[] key){
        JsonArrayBuilder rslt = Json.createArrayBuilder();
        JsonObject temp;
        JsonObjectBuilder element;
        for (int i = 0; i < ary.size(); i++) {
            temp = ary.getJsonObject(i);
            element = Json.createObjectBuilder();
            for (String j : key){
                element.add(j, temp.getString(j));
            }
            rslt.add(element.build());
        }
        return rslt.build();
    }




    public static String getCypherObjectType(JsonArray nodes, JsonArray edges, JsonArray path, String t){
        for (int i = 0; i < nodes.size(); i++){
            if (nodes.getString(i).equals(t))
                return "node";}
        for (int i = 0; i < edges.size(); i++){
            if (edges.getString(i).equals(t))
                return "edge";}
        for (int i = 0; i < path.size(); i++){
            if (path.getString(i).equals(t))
                return "path";}
        throw new RuntimeException();
    }


    public static JsonArray checkAllDBelement(String match, String table, String fieldName) {
        JsonArrayBuilder variable = Json.createArrayBuilder();
        String name;


        try {
            ResultSet rs = getResult(fieldName, table, "\"" + match + "\"");
            while (rs.next()) {
                JsonObjectBuilder temp = Json.createObjectBuilder();

                name = rs.getString("name");
                temp.add("name", name);
                temp.add("source", rs.getString("name"));
                variable.add(temp);


            }


        } catch (SQLException e) {
            e.printStackTrace();
        }


        return variable.build();

    }

    public static JsonObject fieldSchemaCheck(JsonObjectBuilder jObject, String field) {
        String type = null, path = null, model = null;

        String[] partofField = field.split("\\.");
        try {
            ResultSet rs = getResult("name", "schematable", "\"" + partofField[1] + "\"");

            while (rs.next()) {

                type = rs.getString("type");
                path = rs.getString("path");


                String[] pathComponent = path.split("\\.");

                int size = pathComponent.length;
                if (size > 0) {

                    if ((pathComponent[size - 1]).equals("*")) {
                        model = "nested";

                    } else if ((pathComponent[size - 1]).equals("+")) {
                        model = "nested";

                    } else {
                        model = "flat";

                    }
                } else {
                    model = "flat";

                }
                jObject.add("model", model);
                jObject.add("DataType", type);
                jObject.add("path", path);


            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return jObject.build();


    }


    public static JsonObjectBuilder CheckDBStorage(JsonObjectBuilder dbObject, String match) {


        JsonArrayBuilder store = Json.createArrayBuilder();

        String name;

        try {
            ResultSet rs = getResult("name", "storageType", "\"" + match + "\"");
            while (rs.next()) {
                name = rs.getString("type");


                rs = getResult("name", "provider", "\"" + name + "\"");
                while (rs.next()) {
                    String storename = rs.getString("type");
                    store.add(storename);
                }

                dbObject.add(name, store.build());
                dbObject.add("source", match);

            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return dbObject;
    }

    public static JsonObjectBuilder ImportLibraryDBCheck(JsonObjectBuilder retrunObject, String fromClause, JsonArray name) {

        JsonArrayBuilder jproject = Json.createArrayBuilder();

        for (int i = 0; i < name.size(); i++) {
            try {
                ResultSet rs = getResult("name", "libraryentry", name.get(i).toString());
                while (rs.next()) {
                    JsonObjectBuilder jObject = Json.createObjectBuilder();
                    String lname = rs.getString("name");
                    String lpath = rs.getString("path");
                    Integer lsize = rs.getInt("size");
                    String lcomputeClass = rs.getString("computeClass");
                    String type = rs.getString("type");
                    jObject.add("name", lname);
                    jObject.add("VerifiedPath", lpath);
                    jObject.add("size", lsize);
                    jObject.add("ComputeClass", lcomputeClass);
                    jObject.add("type", type);
                    jproject.add(jObject.build());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        retrunObject.add("LOAD", jproject.build());
        return retrunObject;
    }
    public static ResultSet getResult(String fieldName, String table, String value) throws SQLException {

        String name = "*";

        String query = "Select " + name + " from " + table + " where " + fieldName + " = " + value + " ";
       // System.out.println(query);
        Connection con = ParserUtil.dbConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);


        return rs;

    }

    public static ResultSet getResult(String colname, String fieldName, String table, String value) throws SQLException {

        String name = "colname";

        String query = "Select " + name + " from " + table + " where " + fieldName + " = " + value;
        //System.out.println(query);
        Connection con = ParserUtil.dbConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);


        return rs;

    }


    static int insertTuple(String query) {
        Connection con = ParserUtil.dbConnection();
        int count = 0;
        boolean flag = false;

        Statement st = null;
        try {
            st = con.createStatement();
            st.execute(query);
            st.close();
        } catch (SQLException e) {
            System.out.println("" + e.getMessage());
        }

        return 0;
    }


    public static Connection dbConnection() {

        Properties prop = new Properties();
        InputStream input = null;
        String url = null;
        //String url = "jdbc:sqlite:/home/subhasis/IdeaProjects/adil/test.db";


        try {
            input = new FileInputStream("config.properties");
            ;
            try {
                prop.load(input);
            } catch (IOException e) {
                e.printStackTrace();
            }

            url = prop.getProperty("db.path");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //String url = "jdbc:sqlite::memory";
        Connection con = null;
        try {
            con = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println("Internal Database Connection error");
        }
        return con;
    }
    public static Connection sqliteConnect(JsonObject config) {

        //String url = "jdbc:sqlite::memory";
        Connection con = null;
        try {
            con = DriverManager.getConnection(config.getJsonObject("SQLite").getString("path"));
        } catch (SQLException e) {
            System.out.println("Internal Database Connection error");
        }
        return con;
    }

    public static List getStoreCapabilty(String fieldName, String value) throws SQLException {
        List name = new ArrayList();
        String query = "Select capability from StoreCapabilityTable where " + fieldName + " = " + value;
        Connection con = ParserUtil.dbConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        while (rs.next()) {
            name.add(rs.getString("capabitity"));
        }

        return name;

    }

    public static String getOperatorCapabilty(String fieldName, String orgModel, String value) throws SQLException {

        String name = null;

        String query = "Select name from OperatorCapabityTable where " + fieldName + " = " + value;
        Connection con = ParserUtil.dbConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        while (rs.next()) {
            name = rs.getString("name");
        }

        return name;

    }

    public static Integer generateUniqueID() {
        AtomicInteger counter = new AtomicInteger();
        return counter.intValue();
    }

    public static JsonObjectBuilder variableType(JsonObjectBuilder type, String src, String variable, JsonObjectBuilder decesion) {


        String[] temp = variable.split("\\.");

        String fieldType = temp[0];
        String modifiedField = variable.replace(temp[0], src);

        type.add("Type", fieldType);
        type.add("field", modifiedField);
        decesion.add(modifiedField, false);

        return type;


    }

    public static JsonObject getProjectionObject(JsonObject jSlection, List<String> projection, String src, JsonObjectBuilder variable, JsonObjectBuilder decision, JsonObjectBuilder schema, JsonObjectBuilder type) {

        JsonObjectBuilder jwhere = Json.createObjectBuilder(jSlection);
        JsonObjectBuilder jobject = Json.createObjectBuilder();


        JsonArrayBuilder jproject = Json.createArrayBuilder();

        JsonObjectBuilder projectionVar = Json.createObjectBuilder();
        jobject.add("SCHEMA", src);
        jobject.add("verified", false);


        for (int i = 0; i < projection.size(); i++) {


            String prjvar = projection.get(i);
            String[] temp = prjvar.split("\\.");
            schema.add(src, false);

            projectionVar.add("tuple", prjvar);
            if (temp.length > 1) {
                projectionVar.add("Type", temp[0]);
                String replacestring = prjvar.replace(temp[0], src);
                projectionVar.add("field", replacestring);


                decision.add(replacestring, false);
                type.add(replacestring, false);


                jproject.add(projectionVar.build());

            }


        }


        jwhere.add("PROJECT", jproject.build());

        jobject.add("SELECT", jwhere.build());


        return jobject.build();

    }

    public static JsonObjectBuilder functionCheck(JsonObjectBuilder fObject, String functionName) {

        ResultSet rs = null;
        JsonObjectBuilder jObject = Json.createObjectBuilder();
        try {
            rs = getResult("name", "functionsignaturetable", "\"" + functionName + "\"");

            JsonArrayBuilder array = Json.createArrayBuilder();


            while (rs.next()) {

                String lname = rs.getString("name");
                String loutput = rs.getString("output");
                String linput = rs.getString("inputtype");
                String lprovider = rs.getString("provider");
                Integer lcost = rs.getInt("cost");

                jObject.add("name", lname);
                jObject.add("inputSchema", linput);
                jObject.add("outputSchema", loutput);
                jObject.add("provider", lprovider);
                jObject.add("cost", lcost);
                array.add(jObject.build());

            }


            jObject.add("info", array.build());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return jObject;

    }

    public static JsonObjectBuilder getAnnotateJSONPLAN(JsonObjectBuilder jobject, List<String> dictionary, List<String> witness, JsonObject jSlection, List<String> projection, String src, JsonObjectBuilder variable, JsonObjectBuilder desision, JsonObjectBuilder schema, JsonObjectBuilder type) throws Exception {

        //JsonObjectBuilder jobject =Json.createObjectBuilder();
        JsonObject source = getProjectionObject(jSlection, projection, src, variable, desision, schema, type);
        JsonObjectBuilder dbcheck = Json.createObjectBuilder();

        JsonArrayBuilder jarray = Json.createArrayBuilder();
        JsonArrayBuilder jwitness = Json.createArrayBuilder();
        for (int w = 0; w < witness.size(); w++) {

            JsonObjectBuilder witneestemp = Json.createObjectBuilder();
            witneestemp.add("source", src);
            witneestemp.add("name", witness.get(w));
            jwitness.add(witneestemp.build());
        }

        JsonObjectBuilder witnessProject = Json.createObjectBuilder();
        if (witness.size() == 0) {
            JsonObjectBuilder witneestemp = Json.createObjectBuilder();
            witneestemp.add("source", src);
            witneestemp.add("name", "*");

            jwitness.add(witneestemp.build());
        }
        witnessProject.add("PROJECT", jwitness.build());
        jobject.add("WITNESS", witnessProject.build());
        if (dictionary.size() > 1) {
            jobject.add("memoize", true);
            jobject.add("MEMOSRC", source);
            for (int i = 0; i < dictionary.size(); i++) {
                JsonObjectBuilder tempJobject = Json.createObjectBuilder();
                ResultSet rs = getResult("name", "libraryentry", "\"" + dictionary.get(i) + "\"");
                while (rs.next()) {
                    String lname = rs.getString("name");
                    String lpath = rs.getString("path");
                    Integer lsize = rs.getInt("size");
                    String lcomputeClass = rs.getString("computeClass");
                    dbcheck.add("name", lname);
                    dbcheck.add("VerifiedPath", lpath);
                    dbcheck.add("size", lsize);
                    dbcheck.add("ComputeClass", lcomputeClass);
                    dbcheck.add("ImportOptimized", false);
                    tempJobject.add("info", dbcheck.build());
                }
                tempJobject.add("SOURCE", "memoize");

                jarray.add(tempJobject.build());
            }
            jobject.add("UNION", jarray.build());


        } else {
            jobject.add("ANNOTATE", dictionary.get(0));
            jobject.add("SOURCE", source);


        }

        //System.out.println(jobject.build().toString());

        return jobject;

    }



    public static JsonObjectBuilder addCypherConstraint(PGTableEntry vte, String type, Integer ID, String lhs, String... rhs){
        JsonObjectBuilder temp = Json.createObjectBuilder();
        if(vte.isExisted(type, lhs)){
            temp.add("type", type).add("id", ID).add("lhs", lhs);
            if (rhs.length!=0){
                temp.add("rhs", rhs[0]);
            }


        }
        else{throw new IllegalArgumentException(type + " does not contain "+lhs);}
        return temp;
    }


public static String mathOperationOutputType(String t1, String t2, String opt) throws IllegalArgumentException{
        if (t1.equals("Undecided")||(t2.equals("Undecided"))) {
            return "Undecided";
        }

        if (t1.equals("Integer") && t2.equals("Integer")){
            return "Integer";
        }
        else if (t1.equals("String") && t2.equals("String") && opt.equals("+")){
            return "String";
        }
        else if (t1.equals("List") && t2.equals("List") && opt.equals("+")){
            return "List";}
        else {throw new IllegalArgumentException("Type incompatible: "+t1 + ", " + t2);}
}


    public JsonObjectBuilder handleawsmfunction(JsonObjectBuilder jobject) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        JsonObjectBuilder p = Json.createObjectBuilder();
        JsonObject stmt = jobject.build();
        Map property = new HashMap();
        //Get the type
        System.out.println(stmt.toString());
        String type = stmt.getString("input");
        String className = null;
        try {
            ResultSet rs = getResult("name", "classTable", "\"" + type + "\"");
            while (rs.next()) {
                className = rs.getString("class");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.runClassLib(property, className);
        return p;
    }

    public static void typeCompatible(String s1, String s2) throws IllegalAccessException{
        if (!s1.equals(s2)){
            throw new IllegalAccessException();
        }
    }

    public void runClassLib(Map property, String ClassName) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class myClass = Class.forName(ClassName);
        Constructor<?> constructor = myClass.getConstructor(Map.class, String.class);
        Object object = constructor.newInstance(property, ClassName);
    }
    public void runClassLib(JsonObject property, String ClassName) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class myClass = Class.forName(ClassName);
        Constructor<?> constructor = myClass.getConstructor(JsonObject.class, String.class);
        Object object = constructor.newInstance(property, ClassName);
    }

    public static JsonArray concatArray(JsonArray arr1, JsonArray arr2) {
        JsonArrayBuilder result = Json.createArrayBuilder();
        for (int i = 0; i < arr1.size(); i++) {
            result.add(arr1.get(i));
        }
        for (int i = 0; i < arr2.size(); i++) {
            result.add(arr2.get(i));
        }
        return result.build();
    }


}

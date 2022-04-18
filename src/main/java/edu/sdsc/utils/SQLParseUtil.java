package edu.sdsc.utils;

import edu.sdsc.datatype.parser.ColumnInfo;
import edu.sdsc.variables.logicalvariables.RelationTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTable;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import javax.json.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.*;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;

public class SQLParseUtil {
//    private static final DBUtils DB_UTILS;
//
//    // config of database connection
//    static {
//        String URL = "jdbc:postgresql://10.128.6.136/%s";
//        String userName = "xiz675";
//        String password = "PyUGQTg6";
//        String dbName = "postgres";
//        DB_UTILS = new DBUtils(URL, userName, password, dbName);
//    }

    // result is a map: alias -> (real col name, table name, data type)
    private static Triple<String> returnKeys = new Triple<>("name", "rawTableName", "rawColName", "objectType");

    public static JsonArray handleSQL(RDBMSUtils DB_UTILS, String sql, VariableTable vt, Integer... block)  {
        JsonArrayBuilder rsult = Json.createArrayBuilder();
        String lname = sql.replaceFirst("\"", "");
        sql = lname.substring(0, sql.length() - 2);
        try {
            Map<String, String> aliasMap = getAliasMap(sql);
            Map<String, String> allTableNames = getTableNameAliasMap(sql);
//            System.out.println("tables:" + allTableNames);
            List<ColumnInfo> output = matchPostgresColumns(DB_UTILS, allTableNames, aliasMap, vt, block);
            for (ColumnInfo c : output) {
                rsult.add(c.toJsonObject());
            }
        }
        catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return rsult.build();
    }

    // only validate and get the schema when it is not local
    // todo: if this is a local table, should add blockID and parentID and set localvariable etc.
    public static JsonArray handleAwsmSQL(String sql, VariableTable vt, Integer... block) {
        JsonArrayBuilder rsult = Json.createArrayBuilder();
        String lname = sql.replaceFirst("\"", "");
        sql = lname.substring(0, sql.length() - 2);
        try {
            Map<String, String> aliasMap = SQLParseUtil.getAliasMap(sql);
            Map<String, String> allTableNames = SQLParseUtil.getTableNameAliasMap(sql);
            List<String> realTableNameList = new ArrayList<>();
            for (String tN : allTableNames.keySet()) {
                realTableNameList.add(getTrueName(allTableNames, tN));
            }
            // if there are lcoal tables, skip the validation
            for (String realTableName: realTableNameList) {
                Integer vID = vt.searchVariable(realTableName, block).first;
                if (vt.isLocal(vID)) {
                    return rsult.build();
                }
            }
            List<ColumnInfo> output = matchAwsmColumns(allTableNames, aliasMap, vt, block);
            for (ColumnInfo c : output) {
                rsult.add(c.toJsonObject());
            }
        }
        catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return rsult.build();
    }

    public static Map<String, String> getAliasMap(String sql) throws JSQLParserException {
        Select stmt = (Select) CCJSqlParserUtil.parse(sql);
        String pattern = "([a-zA-Z_]+\\.)?\\*";
        Map<String, String> map = new HashMap<>();
        for (SelectItem selectItem : ((PlainSelect) stmt.getSelectBody()).getSelectItems()) {
            System.out.println(selectItem.toString());
            if (Pattern.matches(pattern, selectItem.toString())){
                map.put(selectItem.toString(), null);
            }
            else{
                selectItem.accept(new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectExpressionItem item) {
                    if (item.getAlias() == null) {
                        map.put(item.getExpression().toString(), null);
                    } else {
                        map.put(item.getAlias().getName(), item.getExpression().toString());
                    }
                }
            });
            }
//            selectItem.accept(new SelectItemVisitorAdapter() {
//                @Override
//                public void visit(SelectExpressionItem item) {
//                    if (item.toString().equals("*")){
//                        System.out.println("true");map.put("*", null);}
//                    if (item.getAlias() == null) {
//                        map.put(item.getExpression().toString(), null);
//                    } else {
//                        map.put(item.getAlias().getName(), item.getExpression().toString());
//                    }
//                }
//            });
        }

        return map;
    }

//    private static List<String> getAllTableNames(String sql) throws JSQLParserException {
//        Select selectStatement = (Select) CCJSqlParserUtil.parse(sql);
//        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
//        return tablesNamesFinder.getTableList(selectStatement);
//    }

    public static JsonArray usedVariables(String sql, VariableTable vt, Integer... block) {
        Matcher patt = Pattern.compile("\\$[a-zA-Z0-9]*").matcher(sql);
        JsonArrayBuilder result = Json.createArrayBuilder();
        Pair<Integer, Integer> vIDbID;
        while (patt.find()) {
            JsonObjectBuilder variables = Json.createObjectBuilder();
            int start = patt.start();
            int end = patt.end();
            String var = patt.group().substring(1);
            vIDbID = vt.searchVariable(var, block);
            int vID = vIDbID.first;
            int bID = vIDbID.second;
            variables.add("varName", var).add("varID", vID).add("varType", vt.getVarType(vID)).add("start", start).add("end", end);
            if (bID != -1) {
                variables.add("blockID", bID).add("localVariable", "true").add("parentID", vt.getParentID(vID));
            }
            result.add(variables.build());
        }
        return result.build();
    }

    //todo: distinguish awsm one and DB one. If it has a $ sign before it should be a awm table, otherwise it is a DB table
    public static JsonArray usedTables(String sql, boolean isInDB, VariableTable vt, Integer... block) {
        String lname = sql.replaceFirst("\"", "");
        sql = lname.substring(0, sql.length() - 2);
        JsonArrayBuilder tNames = Json.createArrayBuilder();
        try{
            Map<String, String> allTableNames = getTableNameAliasMap(sql);
            for (String alias : allTableNames.keySet()) {
                String trueName = allTableNames.get(alias);
                if ( trueName == null) {
                    trueName = alias;
                }
                if (isInDB) {
                    tNames.add(trueName);
                }
                else{
                    // will determine if this is a local variable in createNode
                    Integer vID = vt.searchVariable(trueName, block).first;
                    tNames.add(vID);
                }
            }
        }
        catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return tNames.build();
    }

    public static JsonArray usedAwsmTables(String sql, VariableTable vt, Integer... block) {
        String lname = sql.replaceFirst("\"", "");
        sql = lname.substring(0, sql.length() - 2);
        JsonArrayBuilder tNames = Json.createArrayBuilder();
        try{
            Map<String, String> allTableNames = getTableNameAliasMap(sql);
            for (String alias : allTableNames.keySet()) {
                String trueName = allTableNames.get(alias);
                if ( trueName == null) {
                    trueName = alias;
                }
                int vID = vt.searchVariable(trueName, block).first;
                tNames.add(vID);
            }
        }
        catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return tNames.build();
    }


    public static Map<String, String> getTableNameAliasMap(String sql) throws JSQLParserException {
        Select selectStatement = (Select) CCJSqlParserUtil.parse(sql);
        MyTableNameFinder tablesNamesFinder = new MyTableNameFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        Map<String, String> aliasMap = new HashMap<>();

        for (String raw : tableList) {
            String[] split = raw.split(" ");
            assert split.length <= 2;
            if (split.length == 1){
                aliasMap.put(split[0], null);
            }
            else {aliasMap.put(split[1], split[0]);}
        }

        return aliasMap;
    }

    // override the original TableNameFinder to get table name alias
    static class MyTableNameFinder extends TablesNamesFinder {
        @Override
        protected String extractTableName(Table table) {
            return table.getFullyQualifiedName()
                    + ((table.getAlias() != null) ? table.getAlias().toString() : "");
        }
    }

    private static String getTrueName(Map<String, String> aliasMap, String name){
        String trueName = aliasMap.get(name);
        if ( trueName == null) {
            trueName = name;
        }
        return trueName;
    }

    private static List<ColumnInfo> matchAwsmColumns(Map<String, String> tableNames, Map<String, String> rawAliasMap, VariableTable vt, Integer... block) {
        List<String> realTableNameList = new ArrayList<>();
        String realTableName;
        List<ColumnInfo> result = new ArrayList<>();
        for (String tN : tableNames.keySet()) {
            realTableNameList.add(getTrueName(tableNames, tN));
        }
        for (String alias : rawAliasMap.keySet()) {
            List<String> temp = new ArrayList<>();
            String rawColName = getTrueName(rawAliasMap, alias);
            if (rawColName.contains("(")) {
                result.add(getSQLFunctionReturnType(rawColName, alias, rawColName));
            }
            else if (rawColName.contains(".")) {
                String tName = rawColName.split("\\.")[0];
                String colName = rawColName.split("\\.")[1];
                if (tableNames.containsKey(tName)) {
                    realTableName = getTrueName(tableNames, tName);
                    temp.add(realTableName);
                    result.addAll(getAswmType(alias, colName, temp , vt, block));
                } else {
                    throw new IllegalArgumentException("no table:" + tName);
                }
            }
            else {
                result.addAll(getAswmType(alias, rawColName, realTableNameList,vt, block));
            }

        }
        return result;



    }



    private static List<ColumnInfo> matchPostgresColumns(RDBMSUtils DB_UTILS, Map<String, String> tableNames, Map<String, String> rawAliasMap, VariableTable vt, Integer... block) {
        String tName;
        List<ColumnInfo> result = new ArrayList<>();
        List<String> realTableNameList = new ArrayList<>();
        String realTableName;String colName;
        for (String tN : tableNames.keySet()) {
            realTableNameList.add(getTrueName(tableNames, tN));
        }
        for (String alias : rawAliasMap.keySet()) {
            List<String> temp = new ArrayList<>();
            String rawColName = getTrueName(rawAliasMap, alias);
            if (rawColName.contains("(")) {
                String funcName = rawColName.split("\\(")[0];
                ColumnInfo col = getSQLFunctionReturnType(funcName, alias, rawColName);
                result.add(col);
            }
            else if (rawColName.contains(".")) {
                tName = rawColName.split("\\.")[0];
                colName = rawColName.split("\\.")[1];
                if (tableNames.containsKey(tName)) {
                    realTableName = getTrueName(tableNames, tName);
                    temp.add(realTableName);
                    result.addAll(getPostgresType(DB_UTILS, alias, colName, temp , vt, block));
                } else {
                    throw new IllegalArgumentException("no table:" + tName);
                }
            }
            else {
                result.addAll(getPostgresType(DB_UTILS, alias, rawColName, realTableNameList, vt, block));
            }

        }
        return result;
    }


    public static ColumnInfo getSQLFunctionReturnType(String rawColName, String alias, String colName)  {
        String funcName = rawColName.split("\\(")[0];
        String query = "Select * from SQLFunctionTable WHERE name = \""+funcName+"\"";
        //System.out.println(query);
        ColumnInfo col;
        String type = null;
        try{
            Connection con = ParserUtil.dbConnection();
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(query);
            type = rs.getString("output");
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        col = new ColumnInfo(alias, "Undecided", colName, type);
        col.setFunction(true);
        return col;
    }


    public static String getSchema(VariableTable vte, String tableName, String columnName, Integer... block) throws IllegalArgumentException{
        String type;
        if (vte.isInVariableTable(tableName, block)) {
            RelationTableEntry rte = checkSystemTable(vte, tableName, block);
            type = rte.getColumnType(columnName);
        }
        else {
            throw new IllegalArgumentException("no relation: "+tableName);
        }
        if (type != null){
            return type;
        }
        else {throw new IllegalArgumentException("no column: " +tableName+'.'+columnName);}



    }
    public static String getDBSchema(RDBMSUtils DB_UTILS, String tableName, String columnName) throws IllegalArgumentException {
        String type;
        if (DB_UTILS.isTableInDB(tableName)) {
            type = DB_UTILS.getColumnDataType(tableName, columnName);
        }
        else {
            throw new IllegalArgumentException("no relation: "+tableName);
        }
        if (type != null){
            return type;
        }
        else{throw new IllegalArgumentException("no column: " +tableName+'.'+columnName);}
    }



    public static RelationTableEntry checkSystemTable(VariableTable vte, String tableName, Integer... block) throws IllegalArgumentException{
        Integer vID = vte.searchVariable(tableName, block).first;
        System.out.println(vID);
        String type = vte.getVarType(vID);
        if (type.equals("Relation")) {
            return (RelationTableEntry) vte.getVariableProperties(vID);
        }
        else {throw new IllegalArgumentException("no such relation");}
    }

//    private static String getSingleAswmType( String tableName, String columnName, VariableTable vt, Integer... block) throws IllegalArgumentException {
//        RelationTableEntry rte = checkSystemTable(vt, tableName, block);
//        return rte.getColumnType(columnName);
//    }

    private static List<ColumnInfo> getAswmType(String alias, String rawColName, List<String> realTableNameList,  VariableTable vt, Integer... block) throws IllegalArgumentException {
        String columnDataType = null;
        List<ColumnInfo> result = new ArrayList<>();
        if (rawColName.equals("*")){
            for (String realTableName : realTableNameList) {
                result.addAll(getAllAwsmTableColTypes(realTableName, vt, block));
            }
        }
        else{
            for (String realTableName : realTableNameList) {
                RelationTableEntry rte = checkSystemTable(vt, realTableName, block);
                columnDataType = rte.getColumnType(rawColName);
                if (columnDataType != null){
                    ColumnInfo col = new ColumnInfo(alias,  realTableName, rawColName, columnDataType);
                    col.setVarID(vt.searchVariable(realTableName, block).first);
                    result.add(col);
                    break;
                }
            }
            if (columnDataType == null){throw new IllegalArgumentException("no column:" + alias);}
        }
        return result;
    }


    private static List<ColumnInfo> getPostgresType(RDBMSUtils DB_UTILS, String alias, String rawColName, List<String> realTableNameList, VariableTable vt, Integer... block) throws  IllegalArgumentException{
        String columnDataType = null;
        List<ColumnInfo> result = new ArrayList<>();
        if (rawColName.equals("*")){
            for (String realTableName : realTableNameList) {
                if (realTableName.contains("$")) {
                    result.addAll(getAllAwsmTableColTypes(realTableName.substring(1, realTableName.length()), vt, block));
                }
                else {
                    result.addAll(getAllPostgreColTypes(DB_UTILS, realTableName));
                }
            }
        }
        else{
            for (String realTableName : realTableNameList) {
                String tempName = null;
                if (realTableName.contains("$")) {
                    tempName = realTableName.substring(1, realTableName.length());
                    RelationTableEntry rte = checkSystemTable(vt, tempName, block);
                    columnDataType = rte.getColumnType(rawColName);
                }
                else {columnDataType = DB_UTILS.getColumnDataType(realTableName, rawColName);}
                if (columnDataType != null){
                    ColumnInfo col = new ColumnInfo(alias,  realTableName, rawColName, columnDataType);
                    if (realTableName.contains("$")) {assert tempName != null; col.setVarID(vt.searchVariable(tempName, block).first);}
                    result.add(col);
                    break;
                }
            }
            if (columnDataType == null){throw new IllegalArgumentException("no column:" + alias);}
        }
        return result;
    }




//    private static List<ColumnInfo> getType(String alias, String rawColName, List<String> realTableNameList,  Pair<VariableTable, Integer>... vte) throws  IllegalArgumentException{
//        String columnDataType = null;
//        List<ColumnInfo> result = new ArrayList<>();
//        if (rawColName.equals("*")){
//            for (String realTableName : realTableNameList) {
//                result.addAll(getAllColTypes(realTableName, vte));
//            }
//        }
//        else{
//            for (String realTableName : realTableNameList) {
//                columnDataType = getSingleType(realTableName, rawColName, vte);
//                if (columnDataType != null){
//                    ColumnInfo col = new ColumnInfo(alias,  realTableName, rawColName, columnDataType);
//                    if (vte.length > 0) {col.setVarID(vte[0].first.searchVariable(realTableName, vte[0].second));}
//                    result.add(col);
//                    break;
//                }
//            }
//            if (columnDataType == null){throw new IllegalArgumentException("no column:" + alias);}
//        }
//        return result;
//    }

    private static List<ColumnInfo> getAllAwsmTableColTypes(String tName, VariableTable vt, Integer... block) {
        Map<String, String> types;
        List<ColumnInfo> result = new ArrayList<>();
        RelationTableEntry rte = checkSystemTable(vt, tName, block);
        types = rte.getSchema();
        for (String colName:types.keySet()){
            ColumnInfo temp = new ColumnInfo(colName, tName, colName,types.get(colName));
            temp.setVarID(vt.searchVariable(tName, block).first);
            result.add(temp);
        }
        return result;
    }


    private static List<ColumnInfo> getAllPostgreColTypes(RDBMSUtils DB_UTILS, String tName){
        Map<String, String> types;
        List<ColumnInfo> result = new ArrayList<>();
        types = DB_UTILS.getColumnTypeMap(tName);
        for (String colName:types.keySet()){
            ColumnInfo temp = new ColumnInfo(colName, tName, colName,types.get(colName));
            result.add(temp);
    }
        return result;
    }




    public static void main(String[] args) throws JSQLParserException {
        String sql1 = "with x as (select a from $table t1) select $aasd, $b from x ";
        System.out.println(getTableNameAliasMap(sql1));
    }




}


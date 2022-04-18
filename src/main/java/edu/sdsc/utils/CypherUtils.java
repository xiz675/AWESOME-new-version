package edu.sdsc.utils;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;

import java.util.*;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class CypherUtils{

//    private Driver driver;
//    public CypherUtils(JsonObject config, String dbname) {
//        JsonObject dbConfig = config.getJsonObject(dbname);
//        String uri = dbConfig.getString("URL");
//        String user = dbConfig.getString("userName");
//        String password = dbConfig.getString("password");
//        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
//    }
//
//
//    public void close() {driver.close();}
//
//    public Driver getDriver() {
//        return driver;
//    }
//
//
//    public Publisher<Record> executeReturnStream(String cypher) {
//        Session session = driver.session();
//        Result result = session.run(cypher);
//        return Flowable.fromIterable(getIterableFromIterator(result));
//    }
//
//    public List<AwesomeRecord> executeReturn(String cypher) {
//        Session session = driver.session();
//        List<AwesomeRecord> result = session.run(cypher).stream().map(AwesomeRecord::new).collect(Collectors.toList());
//        session.close();
//        return result;
//    }
//
////    public static <T> void insertToTable(Publisher<Record> input, Connection toCon, String tableName, List<String> cols) {
////        try {
////            PreparedStatement s2 = toCon.prepareStatement(
////                    "INSERT INTO " + tableName + " ("
////                            + cols.stream().collect(Collectors.joining(", "))
////                            + ") VALUES ("
////                            + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
////                            + ")");
////
////            Flowable.fromPublisher(input).map(
////                    x -> {for (int i = 1; i <= cols.size(); i++)
////                    {s2.setObject(i, x.get(cols.get(i)));}
////                        s2.addBatch(); return null;});
////        }
////        catch (SQLException e) {
////            e.printStackTrace(); }
////    }
//
//    // change this, if this is executeCypher, then you should know the type already. Should change this as Tinkerpop util
//    public void excuteStore(String cypher, String ouptStr, Connection toCon, List<String> cols) throws SQLException {
//        Session session = driver.session();
//        Result result = session.run(cypher);
////        List<String> cols = result.keys();
//        createTable(toCon, ouptStr, cols);
////        StringBuilder sb = new StringBuilder( 1024 );
////        sb.append( "Create table " ).append( ouptStr ).append( " ( " );
////        for ( int i = 0; i < cols.size(); i ++ ) {
////            if ( i > 0 ) sb.append( ", " );
////            sb.append( cols.get(i) ).append( " " ).append( "text" );
////        } // for columns
////        sb.append( " ) " );
////        Statement st = toCon.createStatement();
////        st.execute(sb.toString());
////        st.close();
//        try (PreparedStatement s2 = toCon.prepareStatement(
//                "INSERT INTO " + ouptStr + " ("
//                        + cols.stream().collect(Collectors.joining(", "))
//                        + ") VALUES ("
//                        + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
//                        + ")"
//        )) {
//
//            while (result.hasNext()) {
//                Record record = result.next();
//                for (int i = 1; i <= cols.size(); i++) {
//                    s2.setObject(i,record.get(i-1).asString());
//                }
//                s2.addBatch();
//            }
//
//            s2.executeBatch();
//        }
//    }

//    public static void store() {
//
//
//
//    }

    public static String getExecutionString(String cypher, ExecuteCypherPhysical ope, boolean optimize,
                                            ExecutionVariableTable evt,  ExecutionVariableTable... localVar) {
        Map<Integer, String> varStr = new HashMap<>();
        Map<Integer, Pair<Integer, Integer>> temp = ope.getVarPosition();
        if (temp != null) {
            for (int i : temp.keySet()) {
                varStr.put(i, cypher.substring(temp.get(i).first, temp.get(i).second));
            }
        }
//        Integer tempVar;
//        String tempvarStr;
        for (Integer s : temp.keySet()) {
            Pair<Integer, String> tempVarKey = new Pair<>(s, "*");
            if ((localVar.length>0 && localVar[0].hasTableEntry(tempVarKey)) || evt.hasTableEntry(tempVarKey)) {
                ExecutionTableEntry value = getTableEntryWithLocal(tempVarKey, evt, localVar);
                if (value instanceof AwesomeInteger || value instanceof AwesomeString || value instanceof MaterializedList) {
                    String realVal = value.toCypher();
                    cypher = cypher.replaceAll("\\$" + varStr.get(s), realVal);
                    if (value instanceof MaterializedList) {
                        System.out.println("keywords list size: " + ((MaterializedList) value).getValue().size() );
                    }
                }
            }
        }

        cypher = cypher.replaceAll("\\$", "");
        if (optimize) {
            if (cypher.contains("contains")) {
                // todo: use regular expression to get the whole thing and then
                cypher = containsRewrite(cypher);
            }
        }
//        System.out.println(cypher);
        return cypher;
    }

    // todo : need to have a full cypher parser (it will cost some time)
    private static String containsRewrite(String rawCypher) {
        Pattern c = Pattern.compile("'(.*?)'");
        Matcher matcher = c.matcher(rawCypher);
        List<String> results = matcher.results()
                .map(MatchResult::group)
                .map(s -> "\"" + s.substring(1, s.length()-1)+"\"")
                .collect(Collectors.toList());
        System.out.println(results.size());
        return String.format("CALL db.index.fulltext.queryNodes(\"tweet\",\'%s\') YIELD node RETURN node.text as text;", String.join(" OR ", results));
    }

    public static void main(String[] args) {
//        String s = "\'Ted Cruz\'";
//        System.out.println(String.format("CALL db.index.fulltext.queryNodes(\"tweet\",\'%s\') YIELD node RETURN node.text as text ", s));
//        s = s.replaceAll("\'", "\'\"");
//        System.out.println(s);
        String s = "match (t:Tweet) where t.text contains 'Richard Blumenthal'  OR t.text contains 'Ron Johnson'  OR t.text contains 'Pat Roberts'  OR t.text contains 'Edward J. Markey'  OR t.text contains 'Brian Schatz'  OR t.text contains 'John Boozman'  " +
                "OR t.text contains 'Bill Cassidy'  OR t.text contains 'Todd Young'  OR t.text contains 'Jeanne Shaheen'  OR t.text contains 'Kevin Cramer'  OR t.text contains 'Tammy Duckworth'  OR t.text contains 'Charles E. Schumer'  OR t.text contains 'Tom Cotton'  OR t.text contains 'Debbie Stabenow'  " +
                "OR t.text contains 'Marco Rubio'  OR t.text contains 'Tammy Baldwin'  OR t.text contains 'Mike Lee'  OR t.text contains 'Jeff Merkley'  OR t.text contains 'Rob Portman'  OR t.text contains 'John Cornyn'  " +
                "OR t.text contains 'Ben Sasse'  OR t.text contains 'Mike Rounds'  OR t.text contains 'John Kennedy'  OR t.text contains 'John Barrasso'  OR t.text contains 'Mike Crapo'  OR t.text contains 'Chuck Grassley'  " +
                "OR t.text contains 'Amy Klobuchar'  OR t.text contains 'Christopher A. Coons'  OR t.text contains 'Mitt Romney'  OR t.text contains 'Ted Cruz'  OR t.text contains 'Martin Heinrich'  OR t.text contains 'Mike Braun'  OR t.text contains 'Lamar Alexander' " +
                " OR t.text contains 'Jerry Moran'  OR t.text contains 'John Thune'  OR t.text contains 'Rand Paul'  OR t.text contains 'Cindy Hyde-Smith'  OR t.text contains 'Jacky Rosen'  " +
                "OR t.text contains 'Cory Gardner'  OR t.text contains 'Chris Van Hollen'  OR t.text contains 'Joni Ernst'  OR t.text contains 'Catherine Cortez Masto'  OR t.text contains 'Lisa Murkowski'  OR t.text contains 'Thom Tillis'  " +
                "OR t.text contains 'Lindsey Graham'  OR t.text contains 'Ron Wyden'  OR t.text contains 'Patty Murray'  OR t.text contains 'John Hoeven'  OR t.text contains 'Jack Reed'  OR t.text contains 'Rick Scott'  " +
                "OR t.text contains 'Tom Udall'  OR t.text contains 'Kamala D. Harris'  OR t.text contains 'Josh Hawley'  OR t.text contains 'Sheldon Whitehouse'  OR t.text contains 'Robert Menendez'  OR t.text contains 'Shelley Moore Capito'  " +
                "OR t.text contains 'Elizabeth Warren'  OR t.text contains 'Sherrod Brown'  OR t.text contains 'Richard Burr'  OR t.text contains 'Dianne Feinstein'  OR t.text contains 'Roy Blunt'  " +
                "OR t.text contains 'Kyrsten Sinema'  OR t.text contains 'Dan Sullivan'  OR t.text contains 'Steve Daines'  " +
                "OR t.text contains 'James Lankford'  OR t.text contains 'David Perdue'  OR t.text contains 'Martha McSally'  OR t.text contains 'Tina Smith'  OR t.text contains 'Mitch McConnell'  OR t.text contains 'Michael F. Bennet'  OR t.text contains 'Johnny Isakson'  OR t.text contains 'Doug Jones'  " +
                "return t.text as t;\n";
        System.out.println(containsRewrite(s));



//        Pattern contain = Pattern.compile("[a-zA-Z0-9.]*\\W+contains\\W+(\\w+)");
//        Matcher matcher = contain.matcher("sdd.d contains ssjj and s.d contains x");
//        while (matcher.find()) {
//            System.out.println(matcher.start());
//            System.out.println(matcher.end());
//            String matched = matcher.group();
//            System.out.println(matched);
//        }
    }


}


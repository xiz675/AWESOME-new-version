//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.NERPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import edu.stanford.nlp.ie.AbstractSequenceClassifier;
//import edu.stanford.nlp.ie.crf.CRFClassifier;
//import edu.stanford.nlp.ling.CoreLabel;
//import edu.stanford.nlp.util.Triple;
//
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.*;
//import java.util.*;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//import static edu.stanford.nlp.ling.SentenceUtils.toCoreLabelList;
//
//// todo: it actually can be done in pipeline
//public class ExecuteNER extends BlockExecutionBase {
//    private AbstractSequenceClassifier<CoreLabel> model;
//////    private NERPhysical ope;
////    private VariableTable vt;
////    private List<String> testCorpus;
////    private JsonObject config;
////    private List<AwesomeRecord> input;
//    private List<AwesomeRecord> corpus;
//    private String colName;
//
//    public ExecuteNER(NERPhysical ope, ExecutionVariableTable evt) {
//        // String loc = ope.getExecuteUnit();
//        // this.db_util = new DBUtils(config, loc);
//        this.colName = ope.getColName();
//        Pair<Integer, String> relationID = new Pair<>(ope.getRelationID(), "*");
//        // get Materialized relation
//        MaterializedRelation temp = (MaterializedRelation) getTableEntryWithLocal(relationID, evt);
//        this.corpus = temp.getValue();
//        InputStream in = null;
//        try {
//            in = new FileInputStream("config.properties");
//            Properties con = new Properties();
//            con.load(in);
//            String modelPath = con.getProperty("default_classifier");
//            model = CRFClassifier.getClassifier(modelPath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public ExecuteNER(String modelPath) throws IOException, ClassNotFoundException {
//        model = CRFClassifier.getClassifier(modelPath);
//    }
//
//    // todo: should move the getColumn outside of this operator
//    @Override
//    public MaterializedRelation execute() {
//        List<AwesomeRecord> result = new ArrayList<>();
//        List<String> corpus = new ArrayList<>();
//        for (AwesomeRecord r : this.corpus) {
//            corpus.add((String) r.getColumn(colName));
//        }
//        String[] cols = {"id", "type", "name"};
//        List<NamedEntity> ners = predict(corpus);
//        int i = 0;
//        for (NamedEntity e : ners) {
//            Map<String, Object> re = new HashMap<>();
//            re.put(cols[0], i);
//            re.put(cols[1], e.getEntityType());
//            re.put(cols[2], e.getName());
//            result.add(new AwesomeRecord(re));
//            ++i;
//        }
//        return new MaterializedRelation(result);
////        JsonArray parameter = ope.getParameters();
////        String col = parameter.getJsonObject(1).getString("varName");
////        Connection con = ParserUtil.sqliteConnect(config);
////        try {
////            Statement st = con.createStatement();
////            st.setFetchSize(100);
////            String[] x = col.split("\\.");
////            System.out.println(x[0]);
////            ResultSet rs = st.executeQuery("select " + x[1] + " from " + x[0]);
////            while (rs.next()) {
////                testCorpus.add(rs.getString(1));
////            }
////            st.close();
////            List<NamedEntity> ners = predict(testCorpus);
////            Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////            Statement create = con.createStatement();
////            String ouptStr = vt.getVarName(ouptVar.first);
////            create.execute("CREATE TABLE " + ouptStr + " (id INTEGER, type Varchar, name Varchar)");
////            create.close();
////            String query = " insert into " + ouptStr + "  (entityID, entityType, entityTerm)"
////                    + " values (?, ?, ?)";
////            PreparedStatement preparedStmt = con.prepareStatement(query);
////            for (int i = 0 ; i < ners.size(); i++) {
////                NamedEntity temp = ners.get(i);
////                preparedStmt.setInt(1, i);
////                preparedStmt.setString(2, temp.getEntityType());
////                preparedStmt.setString(3, temp.getName());
////                preparedStmt.execute();
////
////            }
////            con.close();
//////            MaterializedRelation er = new MaterializedRelation();
////    //        er.setValue(new Pair<String, String>("*", ouptStr));
////    //        evt.insertEntry(ouptVar, er);
////            return new MaterializedRelationName(new Pair<>("*", ouptStr));
////        } catch (SQLException e) {
////            e.printStackTrace();
////        }
////        return null;
//    }
//
////    public void generateNER() throws SQLException {
////
////    }
//
//    public List<NamedEntity> predict(String sentence) {
//        List<NamedEntity> result = new ArrayList<>();
//        model.classifyToCharacterOffsets(sentence).forEach(triple -> {
//            String entityType = triple.first();
//            int offset0 = triple.second();
//            int offset1 = triple.third();
//            String name = sentence.substring(offset0, offset1);
//            result.add(new NamedEntity(name, entityType, 0, offset0, offset1));
//        });
//        return result;
//    }
//
//    private List<NamedEntity> predict(List<String> corpus) {
//        List<NamedEntity> result = new ArrayList<>();
//        int i = 0;
//        for (String sentence : corpus) {
//            for (Triple triple : model.classifyToCharacterOffsets(sentence)) {
//                String entityType = (String) triple.first();
//                int offset0 = (Integer) triple.second();
//                int offset1 = (Integer) triple.third();
//                String name = sentence.substring(offset0, offset1);
//                result.add(new NamedEntity(name, entityType, i, offset0, offset1));
//            }
//            ++ i;
//        }
//        return result;
//    }
//
//    public void setModel(AbstractSequenceClassifier<CoreLabel> model) {
//        this.model = model;
//    }
//
////        List<String> testCorpus = new ArrayList<>();
////        testCorpus.add("Trumps family separation policy was flawed from the start watchdog review says.");
////
////
////        System.out.println(ner.predict(testCorpus));
//    }
//
//
//}
//

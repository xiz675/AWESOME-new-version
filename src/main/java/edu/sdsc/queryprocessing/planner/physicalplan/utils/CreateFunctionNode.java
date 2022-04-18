package edu.sdsc.queryprocessing.planner.physicalplan.utils;



import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ExecuteSolrPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators.AutoPhrasePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators.BlackBoxPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators.ColumnToList;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromDocsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.CreateGraphPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.PageRankPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.FileToSQLite;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.GetColumnPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.Store2SQLite;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringFlatPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringJoinPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators.StringReplacePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.CreateDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.FilterDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.SplitDocumentsPhysical;
import edu.sdsc.utils.Pair;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.CreateExecuteSQLNode.getMaterializedNode;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.CreateExecuteSQLNode.getStore2SQLiteNode;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.physicalVar;

public class CreateFunctionNode {
    public static List<PhysicalOperator> getNodes(Operator x, Integer tempID, boolean optimized) throws IOException {
        List<PhysicalOperator> result = new ArrayList<>();
        if (x instanceof NER) {
            result.add(new NERPhysical((NER) x));
        }
        else if (x instanceof PageRank) {
            PageRankPhysical temp = new PageRankPhysical((PageRank) x);
            if (optimized) {
                temp.setTinkerpop(false);
            }
            result.add(temp);
        }
        else if (x instanceof RowNames) {
            result.add(new RowNamesPhysical((RowNames) x));
        }
        else if (x instanceof SumOpe) {
            // add a getColumn operator if there is a column variable as input. For the getColumn, needs to add only non-local variable
            JsonObject temp = ((SumOpe) x).getParameters().getJsonObject(0);
            String type = temp.getString("type");
            if (type.equals("Column")) {
                String[] names = temp.getString("varName").split("\\.");
                // need to know if this is a local variable input
                // set input variable and intermediate id.
                PhysicalOperator columnData = new GetColumnPhysical(temp.getInt("varID"), names[0], names[1]);
                columnData.setInputVar(physicalVar(x.getInputVar()));
                Set<Pair<Integer, String>> tempOutput = new HashSet<>();
                Pair<Integer, String> tempVar = new Pair<>(tempID, "*");
                tempOutput.add(tempVar);
                columnData.setOutputVar(tempOutput);
                tempID = tempID - 1;
                result.add(columnData);
                // set the input of sum as the temporary var, and should set Intermediate varID
                PhysicalOperator sum = new SumPhysical(tempVar, (SumOpe) x, type);
                sum.setIntermediateVarID(tempID);
                result.add(sum);
            }
            else {
                result.add(new SumPhysical((SumOpe) x, type));
            }
        }
        else if (x instanceof AwsmSQL) {
            result.add(new AswmSQLPhysical((AwsmSQL) x));
        }
        else if (x instanceof GetTextMatrixValue) {
            result.add(new GetTextMatrixValuePhysical((GetTextMatrixValue) x));
        }
        else if (x instanceof AutoPhrase) {
            InputStream in = new FileInputStream("adil-parser/config.properties");
            Properties p = new Properties();
            p.load(in);
            JsonArray parameter = ((AutoPhrase) x).getParameters();
            String col = parameter.getJsonObject(0).getString("varName");
            Integer textVar = parameter.getJsonObject(0).getInt("varID");
            String[] tabCol = col.split("\\.");
            String sql = "select \"" + tabCol[1] + "\" from " + tabCol[0];
            String path = p.getProperty("autoPhrase_workpath") + "data/EN/" + textVar+".txt";
            // create a SQLiteToFile operator where the textVar is the variable that contains the
            // stored data, path is the output file path.
            PhysicalOperator storeText = new SQLiteToFile(sql, textVar, path, "txt");
            result.add(storeText);
            // create a Autophrase physical operator where other input variables stay unchange except the text variable.
            // The first variable of the pair becomes the file path and for the output, the first variable becomes output path.
            String outputpath = path + textVar + "/autophrase.txt";
            PhysicalOperator oprt = new AutoPhrasePhysical((AutoPhrase) x, textVar, path, outputpath);
            result.add(oprt);
            PhysicalOperator loadText = new FileToSQLite(outputpath, x.getOutputVar().iterator().next());
            result.add(loadText);
        }
        else if (x instanceof Union) {
            // todo: currently Union only accept a list of lists, if there are other input types, should decide the input types
            //  and translate to different physical operators
            PhysicalOperator oprt = new UnionListPhysical((Union) x);
            result.add(oprt);
        }
        else if (x instanceof Tdm) {
            PhysicalOperator oprt = new TdmPhysical((Tdm) x);
            result.add(oprt);
        }
        else if (x instanceof TopicModel) {
            PhysicalOperator oprt = new TopicModelPhysical((TopicModel) x);
            result.add(oprt);
        }
        else if (x instanceof ExecuteSolr) {
            PhysicalOperator oprt = new ExecuteSolrPhysical((ExecuteSolr) x);
            result.add(oprt);
        }
        else if (x instanceof Report) {
            PhysicalOperator oprt = new ReportPhysical((Report) x);
            result.add(oprt);
        }
        else if (x instanceof CreateHistogram) {
            PhysicalOperator oprt = new CreateHistogramPhysical((CreateHistogram) x);
            result.add(oprt);
        }
        else if (x instanceof LDA) {
            PhysicalOperator oprt = new LDAPhysical((LDA) x);
            result.add(oprt);
        }
        else if (x instanceof Tokenize) {
            JsonArray parameter = ((Tokenize) x).getParameters();
            // todo: for tokenize, the first parameter is the column variable, and this should be the cap on variable for createDocuments
            JsonObject textPara = parameter.getJsonObject(0);
            String col = textPara.getString("varName");
            Integer textRID = textPara.getInt("varID");
            String[] temp = col.split("\\.");
            String rName = temp[0];
            String textCol = temp[1];
            String docID = null;
            String stopwords = null;
            for (int i=1; i < parameter.size(); i++) {
                JsonObject tempObj = parameter.getJsonObject(i);
                String key = tempObj.getString("key");
                if (key.equals("docid")) {docID =  tempObj.getString("varName").split("\\.")[1];}
                else {stopwords = tempObj.getString("value");}
            }
            // it generates an intermediate output
            // todo: for the docID, split it and remove table name
            PhysicalOperator createDoc = new CreateDocumentsPhysical(rName, textCol, docID);
            createDoc.setInputVar(physicalVar(x.getInputVar()));
            createDoc.setCapOnVarID(new Pair<>(textRID, "*"));
            Set<Pair<Integer, String>> tempOutput = new HashSet<>();
            Pair<Integer, String> tempVar = new Pair<>(tempID, "*");
            tempOutput.add(tempVar);
            createDoc.setOutputVar(tempOutput);
            tempID = tempID - 1;
            result.add(createDoc);
            PhysicalOperator splitDoc = new SplitDocumentsPhysical(" ");
            splitDoc.setInputVar(tempOutput);
            splitDoc.setCapOnVarID(tempVar);
            if (stopwords != null) {
                tempVar = new Pair<>(tempID, "*");
                tempOutput = new HashSet<>();
                tempOutput.add(tempVar);
                splitDoc.setOutputVar(tempOutput);
                tempID = tempID - 1;
                result.add(splitDoc);
                PhysicalOperator filterDoc = new FilterDocumentsPhysical(stopwords);
                filterDoc.setInputVar(tempOutput);
                filterDoc.setCapOnVarID(tempVar);
                filterDoc.setIntermediateVarID(tempID);
                filterDoc.setOutputVar(physicalVar(x.getOutputVar()));
                result.add(filterDoc);
            }
            else {
                splitDoc.setIntermediateVarID(tempID);
                splitDoc.setOutputVar(physicalVar(x.getOutputVar()));
//                splitDoc.setIntermediateVarID(tempID);
                result.add(splitDoc);
            }
        }
        else if (x instanceof BuildWordNeighborGraph) {
            // needs to add intermediate operators for function cracking
            // add a filter words to this
            PhysicalOperator collectGraph = new BuildGraphFromDocsPhysical((BuildWordNeighborGraph) x);
            collectGraph.setInputVar(physicalVar(x.getInputVar()));
            Set<Pair<Integer, String>> tempOutput = new HashSet<>();
            tempOutput.add(new Pair<>(tempID, "*"));
            collectGraph.setOutputVar(tempOutput);
            result.add(collectGraph);
            // create a table from graph elements
            // if optimized, create a sqlite table
            // todo: hard code the table name for now and need to modify
            PhysicalOperator node;
            if (optimized) {
                 node = getStore2SQLiteNode("G", tempID, 0);
            }
            // if not optimized, create a postgres table
            else {
                node = getMaterializedNode("G", tempID, "News");
            }
            tempID = tempID - 1;
            node.setIntermediateVarID(tempID);
            node.setOutputVar(physicalVar(x.getOutputVar()));
            result.add(node);
//            createGraph.setIntermediateVarID(tempID);
//            createGraph.setOutputVar();
        }

        // need to change for relation
//        else if (x instanceof BuildGraphFromRelation) {
//            PhysicalOperator oprt = new ConstructGraphPhysical((ConstructTinkerpopGraph) x);
//            result.add(oprt);
//        }
        else if (x instanceof ArrayAggregate) {
            PhysicalOperator oprt = new ArrayAggregatePhysical((ArrayAggregate) x);
            result.add(oprt);
        }
        else if (x instanceof BlackBoxFunc) {
            PhysicalOperator oprt = new BlackBoxPhysical((BlackBoxFunc) x);
            result.add(oprt);
        }
        //else if (x instanceof ExecuteCypher) {
        //    oprt = new ExecuteCypherPhysical((ExecuteCypher) x);
        //}
        else if (x instanceof StringFlat) {
            PhysicalOperator oprt = new StringFlatPhysical((StringFlat) x);
            result.add(oprt);
        }
        else if (x instanceof StringJoin) {
            PhysicalOperator oprt = new StringJoinPhysical((StringJoin) x);
            result.add(oprt);
        }
        else if (x instanceof StringReplace) {
            PhysicalOperator oprt = new StringReplacePhysical((StringReplace) x);
            result.add(oprt);
        }
        // this is the same as getColumn
        else if (x instanceof ToList) {
            ToList t = (ToList) x;
            PhysicalOperator oprt = new GetColumnPhysical(t.getrID(), t.getrName(), t.getcName());
            oprt.setInputVar(physicalVar(x.getInputVar()));
            oprt.setOutputVar(physicalVar(x.getOutputVar()));
            result.add(oprt);
        }
        else {
            PhysicalOperator oprt = new UnknownPhysical(x);
            result.add(oprt);
        }

        return result;
    }
}

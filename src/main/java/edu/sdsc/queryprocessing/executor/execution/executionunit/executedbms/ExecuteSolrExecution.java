//package edu.sdsc.queryprocessing.executor.execution.executionunit.executedbms;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.ExecuteSolrPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.utils.SolrUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//import org.apache.solr.common.params.SolrParams;
//
//import javax.json.JsonObject;
//import java.io.IOException;
//import java.sql.Connection;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static edu.sdsc.utils.DBMSUtils.generateStatement;
//
//// todo: modify this later
//public class ExecuteSolrExecution extends OutputStreamExecutionBase {
//    private SolrUtil db_util;
//    private String statement;
//    private List<String> cols;
//
//    public ExecuteSolrExecution(JsonObject config, ExecuteSolrPhysical ope, VariableTable vte, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        String loc = ope.getExecuteUnit();
//        this.db_util = new SolrUtil(config, loc);
//        this.cols = ope.getCols();
//        this.statement = generateStatement(ope, ope.getStatement(), evt, false, localEvt);
//    }
//
//    public MaterializedRelation execute() {
//        List<AwesomeRecord> result = new ArrayList<>();
//        try {
//            result = db_util.execute(this.statement, this.cols);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return new MaterializedRelation(result);
//    }
//
//    @Override
//    public StreamRelation executeStreamOutput() {
//        SolrDocumentList result = new SolrDocumentList();
//        try {
//            result = db_util.executeRawResult(this.statement, this.cols);
//        } catch (SolrServerException | IOException e) {
//            e.printStackTrace();
//        }
//        return new StreamRelation(result.stream().map(this::processDoc));
//    }
//
//    private AwesomeRecord processDoc(SolrDocument doc) {
//        Map<String, Object> temp = new HashMap<>();
//        for (String col : cols) {
//            temp.put(col, doc.getFirstValue(col));
//        }
//        return new AwesomeRecord(temp);
//    }
//
//}

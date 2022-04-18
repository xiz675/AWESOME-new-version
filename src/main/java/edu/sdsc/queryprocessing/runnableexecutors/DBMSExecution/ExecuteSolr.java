package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ExecuteSolrPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import edu.sdsc.utils.SolrUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import javax.json.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.sdsc.utils.DBMSUtils.generateStatement;

public class ExecuteSolr extends AwesomeStreamOutputRunnable<ExecuteSolrPhysical, AwesomeRecord> {
    private SolrUtil db_util;
    // private ExecuteSolrPhysical ope;
    private String statement;
    private List<String> cols;

    public ExecuteSolr(JsonObject config, ExecuteSolrPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        super(ope);
        // change the execution mode accordingly
        PipelineMode mode = ope.getExecutionMode();
        this.setExecutionMode(mode);
        String loc = ope.getExecuteUnit();
        this.db_util = new SolrUtil(config, loc);
        this.cols = ope.getCols();
        this.statement = generateStatement(ope, ope.getStatement(), evt, false, localEvt);
    }

    @Override
    public void executeBlocking() {
        List<AwesomeRecord> result = new ArrayList<>();
        try {
            result = db_util.execute(this.statement, this.cols);
            System.out.println(result.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (!this.isStreamIn()) {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
        else {
            return new StreamRelation( this.getStreamResult());
        }
    }

    @Override
    public void executeStreamOutput() {
        SolrDocumentList result = new SolrDocumentList();
        try {
            result = db_util.executeRawResult(this.statement, this.cols);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        this.setStreamResult(result.stream().map(this::processDoc));
    }

    private AwesomeRecord processDoc(SolrDocument doc) {
        Map<String, Object> temp = new HashMap<>();
        for (String col : cols) {
            temp.put(col, doc.getFirstValue(col));
        }
        return new AwesomeRecord(temp);
    }
}
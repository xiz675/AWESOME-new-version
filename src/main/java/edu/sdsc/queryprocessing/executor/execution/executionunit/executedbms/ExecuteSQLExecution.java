//package edu.sdsc.queryprocessing.executor.execution.executionunit.executedbms;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.ExecuteSQLPhysical;
//import edu.sdsc.utils.RDBMSUtils;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.util.List;
//import java.util.stream.Collectors;
//import org.jooq.*;
//import org.jooq.impl.DSL;
//
//import static edu.sdsc.utils.DBMSUtils.generateStatement;
//
//public class ExecuteSQLExecution extends OutputStreamExecutionBase {
//    private String sqlStatement;
//    private Connection fromCon;
//    private boolean isLocalRelation = false;
//
//    // actually for each suboperator, I can also create a physical operator for it. So it is fine, the only place
//    // for two constructors is the operators with stream input.  set different table names for each relation in a collection if that is from HLOs
//    public ExecuteSQLExecution(JsonObject config, ExecuteSQLPhysical ope, ExecutionVariableTable evt, Connection sqlCon, ExecutionVariableTable... localEvt) {
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        this.sqlStatement = generateStatement(ope, ope.getStatement(), evt, true, localEvt);
//        String loc = ope.getExecuteUnit();
//        if (loc.equals("*")) {
//            this.fromCon = sqlCon;
//            this.isLocalRelation = true;
//        }
//        else {
//            RDBMSUtils db_util = new RDBMSUtils(config, loc);
//            this.fromCon = db_util.getConnection();
//        }
//    }
//
//
//    @Override
//    public MaterializedRelation execute() {
//        DSLContext create;
//        if (this.isLocalRelation) {
//            create = DSL.using(fromCon, SQLDialect.SQLITE);
//        }
//        else{
//            create = DSL.using(fromCon, SQLDialect.POSTGRES);
//        }
//        List<AwesomeRecord> result = create.resultQuery(this.sqlStatement).stream().map(AwesomeRecord::new).collect(Collectors.toList());
//        // for materialized relation, store it using its name.
//        return new MaterializedRelation(result);
//    }
//
//    @Override
//    public StreamRelation executeStreamOutput() {
////        Connection conn = db_util.getConnection();
//        DSLContext create;
//        if (this.isLocalRelation) {
//            create = DSL.using(fromCon, SQLDialect.SQLITE);
//        }
//        else{
//            create = DSL.using(fromCon, SQLDialect.POSTGRES);
//        }
//        return new StreamRelation(create.resultQuery(this.sqlStatement).stream().map(AwesomeRecord::new));
//    }
//}

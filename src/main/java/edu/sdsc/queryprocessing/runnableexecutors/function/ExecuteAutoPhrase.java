package edu.sdsc.queryprocessing.runnableexecutors.function;


import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelationName;
import edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators.AutoPhrasePhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import edu.sdsc.utils.AutoPhraseUtil;
import edu.sdsc.utils.Pair;
import edu.sdsc.utils.ParserUtil;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

public class ExecuteAutoPhrase extends AwesomeBlockRunnable<AutoPhrasePhysical, Pair<String, String>> {
    public String outStr;
    public Connection con;
    public String col;
    public Integer min_sup;
    public Integer num;

    public ExecuteAutoPhrase(AutoPhrasePhysical auto, JsonObject config, VariableTable vt) {
        super(auto);
        Pair<Integer, String> outVar = auto.getOutputVar().iterator().next();
        JsonArray parameter = auto.getParameters();
        this.outStr = vt.getVarName(outVar.first);
        this.con = ParserUtil.sqliteConnect(config);
        this.col = parameter.getJsonObject(0).getString("varName");
        this.min_sup = parameter.getJsonObject(1).getInt("value");
        this.num = parameter.getJsonObject(2).getInt("value");
    }

    @Override
    public void executeBlocking() {
        AutoPhraseUtil autoPhrase = new AutoPhraseUtil(con, outStr, col, min_sup, num);
        autoPhrase.callScript();
        setMaterializedOutput(new Pair<>("*", outStr));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedRelationName(this.getMaterializedOutput());
    }


}

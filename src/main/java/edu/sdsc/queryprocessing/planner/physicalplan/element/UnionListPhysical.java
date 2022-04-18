package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Union;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.ExeUnionHLList;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class UnionListPhysical extends FunctionPhysicalOperator {
    private Pair<Integer, String> listVarID;

    public UnionListPhysical(Union ope) {
        this.setParameters(ope.getParameters());
        Set<Pair<Integer, String>> input = PlanUtils.physicalVar(ope.getInputVar());
        this.setInputVar(input);
        this.setOutputVar(PlanUtils.physicalVar(ope.getOutputVar()));
        this.setListVarID(input.iterator().next());
        this.setCapOnVarID(this.getListVarID());
        this.setPipelineCapability(PipelineMode.streaminput);
    }

    public void setListVarID(Pair<Integer, String> listVarID) {
        this.listVarID = listVarID;
    }

    public Pair<Integer, String> getListVarID() {
        return listVarID;
    }

    // no direct value
    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        Pair<Integer, String> listVar = this.getListVarID();
        if (this.isStreamInput()) {
            Stream<MaterializedList> input = (Stream<MaterializedList>) getTableEntryWithLocal(listVar, evt, localEvt).getValue();
            return new ExeUnionHLList( input );
        }
        else {
            ExecutionTableEntryMaterialized listInput = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(listVar, evt, localEvt);
            // this is for parallel only, for parallel + pipeline, each pipeline only has one portion of data and no need to merge.
            if (listInput.isPartitioned()) {
                List<List<MaterializedList>> data = listInput.getPartitionedValue();
                //                List<List<Object>> dataValue = new ArrayList<>();
                //                for (MaterializedList i : data) {dataValue.add((List<Object>) i.getValue());}
                return new ExeUnionHLList(mergeData(data));
            }
            else {
                return new ExeUnionHLList((List<MaterializedList>) listInput.getValue());
            }
        }
    }
}

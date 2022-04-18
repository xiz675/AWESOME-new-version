package edu.sdsc.queryprocessing.planner.physicalplan.element.storage;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Store;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.ExeStoreList;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class StoreList extends PhysicalOperator {
    private Pair<Integer, String> storeVarID;
    private String dbName;
    private String tableName;
//    private boolean rowIndex;
//    private boolean columnIndex;
    private boolean index;
    private List<String> columnNames;

    public StoreList(Store store) {
        Set<Pair<Integer, String>> input = PlanUtils.physicalVar(store.getInputVar());
        this.setInputVar(input);
        this.setStoreVarID(new Pair<>(store.getStoreVarId(), "*"));
        this.setCapOnVarID(this.getStoreVarID());
        this.dbName = store.getDbName();
        this.index = store.isIndex();
        this.columnNames = store.getColumnNames();
        this.tableName = store.getTableName();
        this.setPipelineCapability(PipelineMode.streaminput);
//        this.setOutputVar(PlanUtils.physicalVar(store.getOutputVar()));
    }

    public void setStoreVarID(Pair<Integer, String> storeVarID) {
        this.storeVarID = storeVarID;
    }

    public Pair<Integer, String> getStoreVarID() {
        return storeVarID;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean isIndex() {
        return index;
    }

    @Override
    public ExeStoreList createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        ExeStoreList executor;
        Pair<Integer, String> storeVar = this.getStoreVarID();
        boolean isNested = false;
        boolean isHL = false;
        if (this.isStreamInput()) {
            Stream input;
            // if the value is known
            if (directValue) {
                input = (Stream) inputValue;
                // determine if it is a stream of object or a stream of HL
                Object firstValue = input.findFirst().get();
                if (firstValue!=null && firstValue instanceof ExecutionTableEntry) {
                    isHL = true;
                }
            }
            else {
                ExecutionTableEntry temp = getTableEntryWithLocal(storeVar, evt, localEvt);
                if (temp instanceof StreamList) {
                    input = ((StreamList) temp).getValue();
                }
                else {
                    isHL = true;
                    input = ((StreamHLCollection) temp).getValue();
                }
            }
            executor = new ExeStoreList<>(config, this, sqlCon, input, isHL);
        }
        else {
            // todo: add direct value
            ExecutionTableEntry temp = getTableEntryWithLocal(storeVar, evt, localEvt);
            if (temp instanceof MaterializedList) {
                List<Object> materializedList;
                materializedList = (List<Object>) temp.getValue();
                if (materializedList.size() > 0) {
                    Object x = materializedList.get(0);
                    if (x instanceof List) {
                        isNested = true;
                    }
                }
                executor = new ExeStoreList<>(config, this, sqlCon, materializedList, false, isNested);
            }
            else {
                assert temp instanceof MaterializedHLCollection;
                List<Object> materializedList = new ArrayList<>();
                List<ExecutionTableEntry> TEs = ((MaterializedHLCollection) temp).getValue();
                if (TEs.size() > 0) {
                    ExecutionTableEntry first = TEs.get(0);
                    assert first instanceof MaterializedList || first instanceof AwesomeDouble ||
                            first instanceof AwesomeInteger || first instanceof AwesomeString;
                    // if this is a list of list
                    if (first instanceof MaterializedList) {
                        isNested = true;
                    }
                    for (ExecutionTableEntry TE : TEs) {
                        materializedList.add(TE.getValue());
                    }
                }
                executor = new ExeStoreList<>(config, this, sqlCon, materializedList, true, isNested);
            }
        }
        return executor;
    }
}

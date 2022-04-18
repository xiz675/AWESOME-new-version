package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.LDAPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.text.ExeLDA;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CompoundRunnable implements Runnable{

    List<PhysicalOperator> chain;
    ExecutionVariableTable evt;
    JsonObject config;
    VariableTable vt;
    Connection sqlCon;
    boolean optimize;
    Object inputValue;
    boolean directValue;
    ExecutionVariableTable localEvtFromParent = null;
    ExecutionVariableTable localEvt;
    List<ExecutionTableEntry> resultTEs;
    ExecutionTableEntry resultTE;
    Object result;
    CountDownLatch latch;


    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public List<ExecutionTableEntry> getResultsTE() {
        return resultTEs;
    }

    public ExecutionTableEntry getResultTE() {
        return resultTE;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }

    public void setResultTE(ExecutionTableEntry result) {
        this.resultTE = result;
    }

    public void setResultsTE(List<ExecutionTableEntry> results) {
        this.resultTEs = results;
    }

    public CompoundRunnable(List<PhysicalOperator> chain, ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, CountDownLatch latch, ExecutionVariableTable... localEvt) {
        this.chain = chain;
        this.evt = evt;
        this.config = config;
        this.vt = vt;
        this.sqlCon = sqlCon;
        this.optimize = optimize;
        this.inputValue = inputValue;
        this.directValue = directValue;
        if (localEvt.length!=0) {this.localEvtFromParent=localEvt[0];}
        this.localEvt = new ExecutionVariableTable();
        this.latch = latch;
    }

    @Override
    public void run() {
//        localEvt.insertEntry(this.localVar, te);
        // parallel execution, first operator get direct value
        if (directValue) {
            for (int i=0; i<chain.size()-1; i++) {
                PhysicalOperator ope = chain.get(i);
                // create executable, only the first one got directvalue
                AwesomeRunnable executor;
                if (i==0) {
                    if (this.localEvtFromParent != null) {
                        executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, inputValue, directValue, localEvtFromParent, localEvt);
                    }
                    else {
                        executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, inputValue, directValue, localEvt);
                    }
                }
                else {
                    executor = createExecutorWithLocal(ope);
                }

                // run
                executor.run();
                // add to local evt
                Set<Pair<Integer, String>> output = ope.getOutputVar();
                assert output.size() == 1;
                localEvt.insertEntry(output.iterator().next(), executor.createTableEntry());
            }
            PhysicalOperator ope = chain.get(chain.size()-1);
            AwesomeRunnable executor;
            if (chain.size() == 1) {
                if (localEvtFromParent!=null) {
                    executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, inputValue, true, localEvt, localEvtFromParent);
                }
                else {
                    executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, inputValue, true, localEvt);
                }
            }
            else {
                executor = createExecutorWithLocal(ope);
            }
            executor.run();
            this.setResult(executor.getMaterializedOutput());
            this.setResultTE(executor.createTableEntry());
        }
        // sequential execution, no direct value
        else {
            for (int i=0; i<chain.size()-1; i++) {
                PhysicalOperator ope = chain.get(i);
                AwesomeRunnable executor = createExecutorWithLocal(ope);
                executor.run();
                // add to local evt
                Set<Pair<Integer, String>> output = ope.getOutputVar();
                assert output.size() == 1;
                localEvt.insertEntry(output.iterator().next(), executor.createTableEntry());
            }
            PhysicalOperator ope = chain.get(chain.size()-1);
            if (ope instanceof LDAPhysical) {
                LDAPhysical lda = (LDAPhysical) ope;
                ExeLDA executor;
                if (localEvtFromParent!=null) {
                    executor = lda.createExecutor(evt, config, vt, sqlCon, optimize, null, false, localEvtFromParent, localEvt);
                }
                else {
                    executor = lda.createExecutor(evt, config, vt, sqlCon, optimize, null, false, localEvt);
                }
                executor.run();
                this.setResultsTE(executor.createTableEntries());
            }
            else {
                AwesomeRunnable executor = createExecutorWithLocal(ope);
                executor.run();
                this.setResult(executor.getMaterializedOutput());
                this.setResultTE(executor.createTableEntry());
            }
        }
        latch.countDown();
    }

    private AwesomeRunnable createExecutorWithLocal(PhysicalOperator ope) {
        AwesomeRunnable executor;
        if (localEvtFromParent!=null) {
            executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, null, false, localEvtFromParent, localEvt);
        }
        else {
            executor = ope.createExecutor(evt, config, vt, sqlCon, optimize, null, false, localEvt);
        }
        return executor;
    }
}

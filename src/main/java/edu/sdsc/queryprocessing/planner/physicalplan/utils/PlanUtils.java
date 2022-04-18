package edu.sdsc.queryprocessing.planner.physicalplan.utils;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromDocsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromRelationPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.PageRankPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.ExecuteSQLPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.GetColumnPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.CreateDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.FilterDocumentsPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.SplitDocumentsPhysical;
import edu.sdsc.utils.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlanUtils {

    public static Set<Pair<Integer, String>> physicalVar(Set<Integer> vars) {
        Set<Pair<Integer, String>> rsult = new HashSet<>();
        for (Integer s : vars) {
            Pair<Integer, String> crt = new Pair<>(s, "*");
            rsult.add(crt);
        }
        return rsult;
    }

    public static ExecutionTableEntry getTableEntryWithLocal(Pair<Integer, String> key, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        for (ExecutionVariableTable local : localEvt) {
            if (local.hasTableEntry(key)) {
                return local.getTableEntry(key);
            }
        }
        return evt.getTableEntry(key);
    }

    public static boolean isInLocal(Pair<Integer, String> key, ExecutionVariableTable... localEvt) {
        if (localEvt.length == 0) {
            return false;
        }
        else {
            for (ExecutionVariableTable local : localEvt) {
                if (local.hasTableEntry(key)) {
                    return true;
                }
            }
            return false;
        }
    }


    public static List<Pair<Integer, String>> physicalVarFromList(List<Integer> vars) {
        List<Pair<Integer, String>> rsult = new ArrayList<>();
        for (Integer s : vars) {
            Pair<Integer, String> crt = new Pair<>(s, "*");
            rsult.add(crt);
        }
        return rsult;
    }

    public static PipelineMode inputStreamBlockingCap(PhysicalOperator op) {
        PipelineMode raw = op.getPipeCapability();
        if (raw.equals(PipelineMode.block) || raw.equals(PipelineMode.streamoutput)) {
            return raw;
        }
        else if (raw.equals(PipelineMode.pipeline)){
            return PipelineMode.streamoutput;
        }
        else {
            return PipelineMode.block;
        }
    }

    public static PipelineMode outputStreamBlockingCap(PhysicalOperator op) {
        PipelineMode raw = op.getPipeCapability();
        if (raw.equals(PipelineMode.block) || raw.equals(PipelineMode.streaminput)) {
            return raw;
        }
        else if (raw.equals(PipelineMode.pipeline)){
            return PipelineMode.streaminput;
        }
        else {
            return PipelineMode.block;
        }
    }

}

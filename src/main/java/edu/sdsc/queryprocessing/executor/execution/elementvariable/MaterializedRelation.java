package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.utils.Pair;

import java.util.List;

// This stores the location and name for a Awesome table.
public class MaterializedRelation extends ExecutionTableEntryMaterialized<List<AwesomeRecord>> {
    public MaterializedRelation() {

    }
    public MaterializedRelation(List<AwesomeRecord> relation) {
        this.setValue(relation);
    }

}

package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.datatype.execution.Document;

import java.util.List;

public class MaterializedDocuments extends ExecutionTableEntryMaterialized<List<Document>> {
    public MaterializedDocuments(List<Document> docs) {
        setValue(docs);
    }
}

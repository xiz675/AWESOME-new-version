package edu.sdsc.datatype.parser;


import edu.sdsc.variables.logicalvariables.RelationTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Map;

import static edu.sdsc.utils.JsonUtil.mapToJsonArray;

public class AwsmRelation extends AwsmDataType {
    public Map<String, String> schema = null;

    @Override
    public VariableTableEntry toTableEntry(Integer... block) {
        VariableTableEntry rte;
        if (schema == null) {
            rte = new RelationTableEntry(name, block);
        }
        else {
            rte = new RelationTableEntry(name, schema, block);
        }
        return rte;
    }

    @Override
    public JsonObject toJsonObject(Integer vID, Integer... block) {
        JsonObjectBuilder tempJB = Json.createObjectBuilder().add("varName", name).add("varID", vID).add("varType", "Relation");
        if (schema != null) {
            tempJB.add("schema", mapToJsonArray(schema, "colName", "colType"));
        }

        return tempJB.build();
    }

}

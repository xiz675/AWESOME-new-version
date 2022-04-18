package edu.sdsc.datatype.execution;

import edu.sdsc.utils.Pair;
import io.reactivex.rxjava3.core.Flowable;
import org.jooq.Field;
import org.jooq.Record;
import org.reactivestreams.Publisher;

import java.util.*;

// For any function that returns Relation, needs to change the records to AwesomeRecord type.
// For any function that use Relation as input, apply some unified functions here
public class AwesomeRecord {
    private boolean Neo4jRecord = false;
    private boolean JooqRecord = true;
    private boolean TinkerpopResult = false;
    private boolean recordEOF = false;
    // todo: hard code here
    private boolean case1 = false;


    private Record record = null;
    private org.neo4j.driver.Record recordNeo4j = null;
    private Map<String, Object> recordTinkerpop = null;
    private Pair<Pair<String, String>, Integer> case1value = null;

    public AwesomeRecord() {
        recordEOF = true;
    }

    public AwesomeRecord(Record input) {
        record = input;
    }

    public AwesomeRecord(org.neo4j.driver.Record input) {
        Neo4jRecord = true;
        recordNeo4j = input;
    }

    public AwesomeRecord(Pair<Pair<String, String>, Integer> value) {
        case1 = true;
        case1value = value;
    }

    public AwesomeRecord(Map<String, Object> input) {
        TinkerpopResult = true;
        recordTinkerpop = input;
    }

    public boolean isRecordEOF() {
        return recordEOF;
    }

    public Pair<Pair<String, String>, Integer> getValue(){
        assert case1;
        return case1value;
    }

    public Object getColumn(String key) {
        if (Neo4jRecord) {
            return recordNeo4j.asMap().get(key);
        }
        else if (TinkerpopResult) {
            return recordTinkerpop.get(key);
        }
        else if (case1) {
            if (key.equals("word1")) {
                return case1value.first.first;
            }
            else if (key.equals("word2")) {
                return case1value.first.second;
            }
            else {
                return case1value.second;
            }
        }
        else {
            assert JooqRecord;
            return record.get(key);
        }
    }

    public Set<String> getColumnNames() {
        if (Neo4jRecord) {
            return recordNeo4j.asMap().keySet();
        }
        else if (TinkerpopResult) {
            return recordTinkerpop.keySet();
        }
        else if (case1) {
            Set<String> cols = new HashSet<>();
            cols.add("word1");
            cols.add("word2");
            cols.add("count");
            return cols;
        }
        else {
            assert JooqRecord;
            Set<String> fieldNames = new HashSet<>();
            for (Field i : record.fields()) {
                fieldNames.add(i.getName());
            }
            return fieldNames;
        }
    }

    @Override
    public String toString() {
        if (TinkerpopResult) {
            StringBuilder x = new StringBuilder();
            for (String s : recordTinkerpop.keySet()) {
                x.append("s:").append(recordTinkerpop.get(s));
            }
            return x.toString();
        }
        else {
            return "????";
        }
    }
}

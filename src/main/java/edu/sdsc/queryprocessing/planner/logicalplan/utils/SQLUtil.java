package edu.sdsc.queryprocessing.planner.logicalplan.utils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import  edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.ListTableEntry;
import edu.sdsc.variables.logicalvariables.VariableTable;
import edu.sdsc.variables.logicalvariables.VariableTableEntry;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtil {
    public static Map<String, String> handleVar(VariableTable vt, JsonArray varUsed) {
        Map<String, String> rsult = new HashMap<>();
        for (int i = 0; i < varUsed.size(); i++) {
            JsonObject tempJB = varUsed.getJsonObject(i);
            Integer vID = tempJB.getInt("varID");
            VariableTableEntry vte = vt.getVariableProperties(vID);
            if (vte.getValue()!=null) {
                String value = null;
                if (vte.getType() == DataTypeEnum.valueOf("Integer").ordinal()) {
                    value = ((Integer) vte.getValue()).toString();
                }
                else if (vte.getType() == DataTypeEnum.valueOf("String").ordinal()) {
                    value = "'"+ vte.getValue() +"'";
                }
                else if (vte.getType() == DataTypeEnum.valueOf("List").ordinal()) {
                    ListTableEntry temp = (ListTableEntry) vte;
                    if (temp.getElementType() == DataTypeEnum.valueOf("Integer").ordinal()) {
                        ArrayList<Integer> tempVal = (ArrayList<Integer>) vte.getValue();
                        String valueStr = tempVal.stream().map(Object::toString)
                                .collect(Collectors.joining(", "));
                        value = valueStr ;
                    }
                    else if (temp.getElementType() == DataTypeEnum.valueOf("String").ordinal()) {
                        ArrayList<String> tempVal = (ArrayList<String>) vte.getValue();
                        value = "'" + String.join("', '", tempVal) + "'";
                    }
                }
                if (value != null) {
                    rsult.put(tempJB.getString("varName"), value);
                }
            }
        }
        return rsult;
    }

    private static Pattern adilPattern = Pattern.compile("\\$[a-zA-Z0-9.]*");

    public static Map<Integer, Pair<Integer, Integer>> processSQLTemplate(String rawSQL, Map<String, String> varMap, Map<String, Integer> name2id, StringBuilder sb) {
        Map<Integer, Pair<Integer, Integer>> unmatchedMap = new HashMap<>();

        Matcher matcher = adilPattern.matcher(rawSQL);
        int inc = 0;
        while (matcher.find()) {
            String matched = matcher.group().substring(1);
            matched = matched.split("\\.")[0];
            String replaced = varMap.get(matched);
            if (replaced != null) {
                matcher.appendReplacement(sb, replaced);
                inc += replaced.length() - matched.length() - 1;
            } else {
                Integer id = name2id.get(matched);
                unmatchedMap.put(id, new Pair<>(matcher.start() + inc + 1, matcher.end() + inc));
            }
        }
        matcher.appendTail(sb);

        return unmatchedMap;
    }




    public static void main(String args[]) {
        String s= "\''asd\''";
        System.out.println(s.substring(1, s.length()-1));
    }
}

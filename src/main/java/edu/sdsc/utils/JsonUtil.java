package edu.sdsc.utils;

import edu.sdsc.utils.Triple;

import javax.json.*;
import java.util.*;

public class JsonUtil {

    public static JsonArray tripleListToJsonArray(List<Triple<String>> x, Triple<String> names){
        JsonArrayBuilder result = Json.createArrayBuilder();
        JsonObjectBuilder temp;
        for (Triple<String> i: x) {
            temp = Json.createObjectBuilder().add(names.getFirst(), i.getFirst())
                    .add(names.getSecond(), i.getSecond())
                    .add(names.getThird(), i.getThird())
                    .add(names.getFourth(), i.getFourth());
            result.add(temp.build());
        }
        return result.build();
    }

    public static Map<String, String> jsonArrayToMap(JsonArray ja, String key, String value){
        Map<String, String> rslt = new HashMap<>();
        for (int i = 0; i < ja.size(); i++) {
            JsonObject t = ja.getJsonObject(i);
            String keyName = t.getString(key);
            String valueName = t.getString(value);
            if (keyName!=null){
                rslt.put(keyName, valueName);
            }
        }
        return rslt;
    }

    public static JsonArray jsonArrayToJsonSetArray(JsonArray ja){
        Set<String> temp = new HashSet<>();
        JsonArrayBuilder rslt = Json.createArrayBuilder();
        for (int i = 0; i < ja.size(); i++) {
            temp.add(ja.getString(i));
        }
        for (String s : temp) {
            rslt.add(s);
        }
        return rslt.build();
    }


    public static JsonArray mapToJsonArray(Map<String, String> map, String key, String value){
        JsonArrayBuilder result = Json.createArrayBuilder();
        for (String s : map.keySet()) {
            JsonObject temp = Json.createObjectBuilder().add(key, s).add(value, map.get(s)).build();
            result.add(temp);
        }
        return result.build();
    }

    public static List<Integer> jsonArrayToIntList(JsonArray js) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < js.size(); i++) {
            result.add(js.getInt(i));
        }
        return result;
    }
    public static List<String> jsonArrayToStringList(JsonArray js) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < js.size(); i++) {
            result.add(js.getString(i));
        }
        return result;
    }

    public static Set<Integer> jsonArrayToIntSet(JsonArray js) {
        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < js.size(); i++) {
            result.add(js.getInt(i));
        }
        return result;
    }


    public static JsonArray arrayToJsonArray(List<String> x) {
        JsonArrayBuilder js = Json.createArrayBuilder();
        for (String i : x) {
            js.add(i);
        }
        return js.build();
    }

    // from a jsonArray of JsonObject, get the string array with given key
    public static List<String> jsonArrayToStringListWithKey(JsonArray js, String key) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < js.size(); i++) {
            result.add(js.getJsonObject(i).getString(key));
        }
        return result;

    }

}

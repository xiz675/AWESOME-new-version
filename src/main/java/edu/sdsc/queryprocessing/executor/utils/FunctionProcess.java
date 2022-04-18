package edu.sdsc.queryprocessing.executor.utils;

import edu.sdsc.datatype.execution.FunctionParameter;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FunctionProcess {
    public static List<FunctionParameter> translateParameters(JsonArray parameters) {
        List<FunctionParameter> parm = new ArrayList<>();
        for (int i=0; i<parameters.size(); i++) {
            parm.add(new FunctionParameter(parameters.getJsonObject(i)));
        }
        return parm;
    }

    public static FunctionParameter getParameterWithKey(List<FunctionParameter> parameters, String key) {
        for (FunctionParameter temp : parameters) {
            if (temp.isHasKey() && temp.getKey().equals(key)) {
                return temp;
            }
        }
        return null;
    }



}

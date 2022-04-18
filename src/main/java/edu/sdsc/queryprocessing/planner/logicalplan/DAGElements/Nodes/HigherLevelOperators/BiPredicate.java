package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators;

import javax.json.JsonObject;

public class BiPredicate extends HighLevelOperator {
    private String biOpe;
    private Integer leftOpeID;
    private Integer rightOpeID;
    private JsonObject left;
    private JsonObject right;



    public BiPredicate(JsonObject operator, Integer leftOpeID, Integer rightOpeID) {
        this.biOpe = operator.getString("operator");
        this.left = operator.getJsonObject("leftOperatee");
        this.right = operator.getJsonObject("rightOperatee");
        this.setLeftOpeID(leftOpeID);
        this.setRightOpeID(rightOpeID);
//        this.leftOpe = leftOpe;
//        this.rightOpe = rightOpe;
//        setReturnOperator(leftOpe || rightOpe);
//        if (isReturnOperator()) {
//            setOutputOperator(vID);
//        }
    }


//    public void detailSetting(Integer vID){
//        if (left.containsKey("RHS")) {
//            leftOpe = true;
//        }
//        if (right.containsKey("RHS")) {
//            rightOpe = true;
//        }
//        setReturnOperator(leftOpe || rightOpe);
//        if (isReturnNode()) {
//            setOutputOperator(vID);
//        }
//    }


    public String getBiOpe() {
        return biOpe;
    }

    public JsonObject getLeft() {
        return left;
    }

    public JsonObject getRight() {
        return right;
    }

    public void setLeftOpeID(Integer leftOpeID) {
        this.leftOpeID = leftOpeID;
    }

    public void setRightOpeID(Integer rightOpeID) {
        this.rightOpeID = rightOpeID;
    }

    public Integer getLeftOpeID() {
        return leftOpeID;
    }

    public Integer getRightOpeID() {
        return rightOpeID;
    }
}

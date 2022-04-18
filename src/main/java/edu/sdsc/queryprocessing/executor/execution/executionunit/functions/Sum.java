//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions;
//
//import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
//import io.reactivex.rxjava3.core.Flowable;
//import org.ejml.data.DMatrixRMaj;
//import org.jooq.Record;
//import org.reactivestreams.Publisher;
//
//import java.util.List;
//
//public class Sum {
//    private enum types {List, Column, MatrixColumn, MatrixRow, ListStream, RecordStream}
//    private types type;
////    private boolean isRecordStream = false;
//
//    public Sum() {}
//
//    public Sum(FunctionPhysicalOperator ope) {
//
//    }
//
////    public SumOpe(Publisher<Record> recordStream) {
////        this.recordStream = recordStream;
////        this.isStream = true;
////        this.type = types.RecordStream;
////    }
////
////
////
////
//    public double getSumResult(Publisher<Record> recordStream, boolean record) {
//        assert record = true;
//        Flowable<Double> x = Flowable.fromPublisher(recordStream).map(i -> (double) i.getValue(0));
//        return x.reduce(Double::sum).blockingGet();
//    }
//
//    public double getSumResult(Publisher<Double> listStream ) {
//        return Flowable.fromPublisher(listStream).reduce(Double::sum).blockingGet();
//    }
//
//
//    public double getSumResult(List<Double> listData) {
//        double rsult = 0;
//        for (double x : listData) {
//            rsult += x;
//        }
//        return rsult;
//
//    }
//
//    public double getSumResult(DMatrixRMaj doubleMatrix, int index, boolean isRow) {
//        int numofRows = doubleMatrix.getNumRows();
//        int numofCol = doubleMatrix.getNumCols();
//        double result = 0;
//        if (isRow) {
//            assert index < numofRows;
//            for (int i = 0; i < numofCol; i++) {
//                result += doubleMatrix.get(index, i);
//            }
//            return result;
//        }
//        else {
//            assert index < numofCol;
//            for (int i = 0; i < numofRows; i++) {
//                result += doubleMatrix.get(i, index);
//            }
//            return result;
//        }
//    }
//
//
//}

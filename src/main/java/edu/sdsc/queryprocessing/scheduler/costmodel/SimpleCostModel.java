package edu.sdsc.queryprocessing.scheduler.costmodel;

public class SimpleCostModel {

    public double[] makePolynomialFeature(double[] f) {
        // for a single feature, create the polynomial value
        int size = 1 + f.length + f.length*(f.length-1)/2 + f.length;
        double[] feat = new double[size];
        feat[0] = 1;
        for (int i=0; i< f.length; i++) {
            feat[i+1] = f[i];
        }
        int index = 1+f.length;
        for (int i=0; i< f.length; i++) {
            for (int j=i; j<f.length; j++) {
                feat[index] = f[i] * f[j];
                index+=1;
            }
        }
        return feat;
    }

    // compute the cost of each single executor given the coefficient and parameters
    public double computeCost(double[] f, double[] coef) {
        double[] pf = makePolynomialFeature(f);
        assert pf.length == coef.length;
        double value = 0;
        for (int i=0; i<f.length; i++) {
            value = value + f[i] * pf[i];
        }
        return value;
    }



}

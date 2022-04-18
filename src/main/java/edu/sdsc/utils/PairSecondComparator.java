package edu.sdsc.utils;

import java.util.Comparator;

public class PairSecondComparator implements Comparator<Pair<Integer, Long>> {
    @Override
    public int compare(Pair<Integer, Long> t1, Pair<Integer, Long> t2) {
        if (t1.second < t2.second) {
            return -1;
        }
        else if (t1.second > t2.second) {
            return 1;
        }
        return 0;
    }
}

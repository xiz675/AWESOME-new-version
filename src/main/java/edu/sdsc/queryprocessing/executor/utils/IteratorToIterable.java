package edu.sdsc.queryprocessing.executor.utils;

import java.util.Iterator;

public class IteratorToIterable {
    public static <T> Iterable<T> getIterableFromIterator(Iterator<T> iterator) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator()
            {
                return iterator;
            }
        };
    }
}

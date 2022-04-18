package edu.sdsc.utils;

import java.util.Objects;

public class Pair<U, V> {
    public final U first;    // first field of a Pair
    public final V second;    // second field of a Pair

    // Constructs a new Pair with specified values
    public Pair(U first, V second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) &&
                Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
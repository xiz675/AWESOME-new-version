package edu.sdsc.queryprocessing.executor.utils;


import cc.mallet.types.Instance;
import edu.sdsc.datatype.execution.Document;
import io.reactivex.rxjava3.core.Flowable;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

// this is for mallet
public class StreamInstanceIterator implements Iterator<Instance> {
    Iterator<Document> subIterator;
    Object target;
    int index;

    public StreamInstanceIterator(Stream<Document> doc) {
        this.subIterator = doc.iterator();
        this.target = null;
        this.index = 0;
    }

    public Instance next ()
    {
        URI uri = null;
        try { uri = new URI ("document:" + index++); }
        catch (Exception e) { e.printStackTrace(); throw new IllegalStateException(); }
        return new Instance (String.join(" ", subIterator.next().tokens), target, uri, null);
    }

    public boolean hasNext ()	{	return subIterator.hasNext();	}

    public void remove() { subIterator.remove(); }
}

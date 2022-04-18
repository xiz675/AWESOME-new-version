package edu.sdsc.queryprocessing.executor.utils;

import cc.mallet.types.Instance;
import edu.sdsc.datatype.execution.Document;

import javax.print.Doc;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class DocArrayIterator implements Iterator<Instance>{

    Iterator<Document> subIterator;
    Object target;

    public DocArrayIterator (List<Document> data, Object target)
    {
        this.subIterator = data.iterator();
        this.target = target;
    }

    public DocArrayIterator (List<Document> data)
    {
        this (data, null);
    }

    public DocArrayIterator (Document[] data, Object target)
    {
        this (java.util.Arrays.asList (data), target);
    }

    public DocArrayIterator (Document[] data)
    {
        this (data, null);
    }


    public Instance next ()
    {
//        URI uri = null;
//        try { uri = new URI ("array:" + index++); }
//        catch (Exception e) { e.printStackTrace(); throw new IllegalStateException(); }
        Document doc = subIterator.next();
        return new Instance (doc.text, target, "array:" + doc.docID.toString(), null);
    }

    public boolean hasNext ()	{	return subIterator.hasNext();	}

    public void remove() { subIterator.remove(); }

}

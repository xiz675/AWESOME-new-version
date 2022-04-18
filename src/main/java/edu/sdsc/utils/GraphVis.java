package edu.sdsc.utils;

import com.mxgraph.layout.*;
import com.mxgraph.util.mxCellRenderer;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;

public class GraphVis {
    public static <V, E> void vis(Graph<V, E> graph, String layout, String fileName) throws IOException {
        JGraphXAdapter<V, E> adapter = new JGraphXAdapter<>(graph);
        mxIGraphLayout graphLayout;

        switch (layout) {
            case "circle":
            case "circleLayout":
                graphLayout = new mxCircleLayout(adapter);
                break;
            case "compactTreet":
            case "compactTreetLayout":
                graphLayout = new mxCompactTreeLayout(adapter);
                break;
            case "edgeLabel":
            case "edgeLabelLayout":
                graphLayout = new mxEdgeLabelLayout(adapter);
                break;
            case "organic":
            case "organicLayout":
                graphLayout = new mxOrganicLayout(adapter);
                break;
            case "fastOrganic":
            case "fastOrganicLayout":
                graphLayout = new mxFastOrganicLayout(adapter);
                break;
            case "parallelEdge":
            case "parallelEdgeLayout":
                graphLayout = new mxParallelEdgeLayout(adapter);
                break;
            case "partition":
            case "partitionLayout":
                graphLayout = new mxPartitionLayout(adapter);
                break;
            case "stack":
            case "stackLayout":
                graphLayout = new mxStackLayout(adapter);
                break;
            default:
                throw new RuntimeException(String.format("Unsupported graph layout: %s.", layout));
        }

        graphLayout.execute(adapter.getDefaultParent());
        File imgFile = new File(fileName);
        BufferedImage image = mxCellRenderer.createBufferedImage(adapter, null, 2, Color.WHITE, true, null);
        ImageIO.write(image, "PNG", imgFile);

        if (imgFile.exists()) {
            System.out.println("Image creation success.");
        } else {
            System.out.println("Image creation fail.");
        }
    }

    static class MyNode {
        String name;

        MyNode(String name) {
            this.name = name;
        }

        // put the message you want to show in this method
        // note that, if you want to customize message shown on the edge, you should override the toString method of
        // your own edge class
        @Override
        public String toString() {
            return name;
        }
    }


    public static void main(String[] args) throws IOException {
    }
}

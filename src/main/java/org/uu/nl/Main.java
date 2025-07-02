// package org.uu.nl;

// import org.xml.sax.SAXException;

// import java.io.IOException;
// import java.security.NoSuchAlgorithmException;

// import org.apache.jena.query.ARQ;
// import org.rdfhdt.hdt.hdt.HDT;
// import org.rdfhdt.hdt.hdt.HDTManager;
// import org.rdfhdt.hdtjena.HDTGraph;


// public class Main {
// // public class PreGenerateIndex { 

//     public static void main(String[] args) throws IOException, SAXException, NoSuchAlgorithmException {
//         if ( args.length != 1 ) {
//             System.err.println("Usage: PreGenerateIndex <file.hdt>");
//             System.exit(1);
//         }
//         String hdtFile = args[0];

//         // 1) Initialize Jena’s global Context
//         ARQ.init();

//         // 2) Map (and if necessary build) the .hdt.index
//         System.out.println("Building .hdt.index for " + hdtFile);
//         HDT hdt = HDTManager.mapIndexedHDT(hdtFile, null);

//         // 3) Wrap in a Jena Graph (optional)
//         HDTGraph graph = new HDTGraph(hdt);

//         // 4) Close to flush and finish index generation
//         graph.close();
//         System.out.println("Done.");
//     }
//     // public static void main(String[] args) throws IOException, SAXException, NoSuchAlgorithmException {
//     //     ARQ.init();
//     //     // Example args: java -Xmx16g -jar dblp.jar dblp-2017-01-24.hdt dblp-2017-01-01.xml dblp-2016-10-01.dtd 2014 2017
//     //     Ambiguator ambiguator = new AmbiguateDBLP(args[0], args[1], args[2], args[3] , args[4]);
//     //     ambiguator.ambiguate();
//     // }
// }


package org.uu.nl;

import org.apache.jena.query.ARQ;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;
import org.xml.sax.SAXException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class Main {
    public static void main(String[] args)
        throws IOException, SAXException, NoSuchAlgorithmException {
      if (args.length == 1) {
        // index‐builder mode
        String hdtFile = args[0];
        ARQ.init();
        System.out.println("Building .hdt.index for " + hdtFile);
        HDT hdt = HDTManager.mapIndexedHDT(hdtFile, null);
        new HDTGraph(hdt).close();
        System.out.println("Done.");
      }
      else if (args.length == 6) {
        // ambiguity mode
        Ambiguator ambiguator = new AmbiguateDBLP(
          args[0], args[1], args[2], args[3], args[4], args[5]
        );
        ambiguator.ambiguate();
      }
      else {
        System.err.println("Usage:\n" +
          "  To build index: Main <file.hdt>\n" +
          "  To run pipeline: Main <hdt> <xml> <dtd> <startYear> <endYear> <noisePct>\n" +
          "    <noisePct> integer 0-100, Percentage of authors' names randomly replaced");
        System.exit(1);
      }
    }
}


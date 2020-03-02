package org.uu.nl;


import org.xml.sax.SAXException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class Main {


    public static void main(String[] args) throws IOException, SAXException, NoSuchAlgorithmException {
        // Example args: java -Xmx16g -jar dblp.jar dblp-2017-01-24.hdt dblp-2017-01-01.xml dblp-2016-10-01.dtd 2014 2017
        Ambiguator ambiguator = new AmbiguateDBLP(args[0], args[1], args[2], args[3] , args[4]);
        ambiguator.ambiguate();
    }
}

package org.uu.nl;


import org.rdfhdt.hdt.exceptions.ParserException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class Main {


    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, ParserException {

        ModifyHDT hdt = new ModifyHDT(args[0], args[1]);
    }
}

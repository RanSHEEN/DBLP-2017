package org.uu.nl;

import me.tongfei.progressbar.ProgressBar;
import org.apache.jena.rdf.model.*;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTFactory;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class ModifyHDT {

    public ModifyHDT(String file, String year) throws IOException, NoSuchAlgorithmException, ParserException {

        final int minYear = Integer.parseInt(year);
        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

        System.out.println("Loading HDT file");
        final HDT hdt = HDTManager.loadIndexedHDT(file, null);
        final HDTGraph graph = new HDTGraph(hdt);
        final Model hdtModel = ModelFactory.createModelForGraph(graph);
        System.out.println("Loaded HDT file");

        final Resource hdtPersonResource = hdtModel.getResource("http://xmlns.com/foaf/0.1/Agent");
        final Property hdtNameProperty = hdtModel.getProperty("http://xmlns.com/foaf/0.1/name");
        final Property hdtTitleProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/title");
        final Property hdtSubjectProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/subject");
        final Property hdtIssuedProperty = hdtModel.getProperty("http://purl.org/dc/terms/issued");
        final Property hdtTypeProperty = hdtModel.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        final Property hdtCreatorProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/creator");
        final Property hdtCitationProperty = hdtModel.getProperty("http://purl.org/dc/terms/references");

        final Model model = ModelFactory.createDefaultModel();
        final Resource personResource = model.createResource("http://xmlns.com/foaf/0.1/Agent");
        final Resource pubResource = model.createResource("http://xmlns.com/foaf/0.1/Document");
        final Property nameProperty = model.createProperty("http://xmlns.com/foaf/0.1/name");
        final Property titleProperty = model.createProperty("http://purl.org/dc/elements/1.1/title");
        final Property subjectProperty = model.createProperty("http://purl.org/dc/elements/1.1/subject");
        final Property issuedProperty = hdtModel.createProperty("http://purl.org/dc/terms/issued");
        final Property creatorProperty = model.createProperty("http://purl.org/dc/elements/1.1/creator");
        final Property citationProperty = model.createProperty("http://purl.org/dc/terms/references");

        ResIterator persons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

        long nrOfPersons = iteratorSize(persons);

        persons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

        try(ProgressBar pb = new ProgressBar("Processing persons", nrOfPersons, 100)) {
            while(persons.hasNext()) {

                final Resource hdtPerson = persons.nextResource();
                final ResIterator publications = hdtModel.listResourcesWithProperty(hdtCreatorProperty, hdtPerson);

                final String name = hdtPerson.getProperty(hdtNameProperty).getLiteral().getString().replaceAll("\\d","").trim();

                while(publications.hasNext()) {

                    final Resource hdtPublication = publications.nextResource();

                    final int issued = Integer.parseInt(hdtPublication.getProperty(hdtIssuedProperty).getLiteral().getString());
                    if(issued < minYear) continue;;

                    messageDigest.update(hdtPublication.getURI().getBytes());
                    final Resource person = model.createResource(hdtPerson.getURI() + "/" + Base64.getUrlEncoder().encodeToString(messageDigest.digest()), personResource);
                    person.addLiteral(nameProperty, name);

                    final Resource publication = model.createResource(hdtPublication.getURI(), pubResource);

                    final Statement titleStatement = hdtPublication.getProperty(hdtTitleProperty);
                    if(titleStatement != null) publication.addLiteral(titleProperty, titleStatement.getLiteral().getString());

                    final Statement subjectStatement = hdtPublication.getProperty(hdtSubjectProperty);
                    if(subjectStatement != null) publication.addLiteral(subjectProperty, subjectStatement.getLiteral().getString());

                    publication.addLiteral(issuedProperty, issued);
                    publication.addProperty(creatorProperty, person);

                    final StmtIterator references = hdtPublication.listProperties(hdtCitationProperty);

                    while(references.hasNext()) {
                        final Resource hdtCitation = references.nextStatement().getResource();
                        final Resource citation = model.createResource(hdtCitation.getURI(), pubResource);
                        publication.addProperty(citationProperty, citation);
                    }
                }
                pb.step();
            }
        }
        System.out.println("Generating new HDT");
        HDT outputHDT = HDTManager.generateHDT(
                new ModelTripleStringIterator(model),
                "org.uu.nl.dblp",
                new HDTSpecification(),
                null );
        System.out.println("Generating HDT index");
        outputHDT = HDTManager.indexedHDT(outputHDT, null);

        System.out.println("Writing to file");
        outputHDT.saveToHDT("data/dblp-"+minYear+"-2017.hdt", null);
    }

    private <T> long iteratorSize(Iterator<T> it) {
        Iterable<T> newIterable = () -> it;
        return StreamSupport.stream(newIterable.spliterator(), true).count();
    }

    private static class ModelTripleStringIterator implements Iterator<TripleString> {

        private StmtIterator stmtIterator;

        private ModelTripleStringIterator(Model model) {
            this.stmtIterator = model.listStatements();
        }

        @Override
        public boolean hasNext() {
            return stmtIterator.hasNext();
        }

        @Override
        public TripleString next() {
            final Statement statement = stmtIterator.nextStatement();
            return new TripleString(
                    statement.getSubject().toString(),
                    statement.getPredicate().toString(),
                    statement.getObject().toString());
        }
    }
}

package org.uu.nl;

import me.tongfei.progressbar.ProgressBar;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.dblp.mmdb.Person;
import org.dblp.mmdb.PersonName;
import org.dblp.mmdb.RecordDb;
import org.dblp.mmdb.RecordDbInterface;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class AmbiguateDBLP implements Ambiguator {

    private final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
    private final RecordDbInterface xmlModel;
    private final Resource hdtPersonResource;
    private final Property hdtNameProperty;
    private final Property hdtTitleProperty;
    private final Property hdtIssuedProperty;
    private final Property hdtTypeProperty;
    private final Property hdtCreatorProperty;
    private final Property hdtCitationProperty;
    private final Resource agentResource;
    private final Resource personResource;
    private final Resource pubResource;
    private final Model model;
    private final Property typeProperty;
    private final Property idProperty;
    private final Property nameProperty;
    private final Property titleProperty;
    private final Property issuedProperty;
    private final Property creatorProperty;
    private final Property citationProperty;
    private final long minYear;
    private final long maxYear;
    private final Model hdtModel;

    public AmbiguateDBLP(String hdtFilename, String dblpXmlFilename, String dblpDtdFilename, String yearFrom, String yearTo) throws IOException, NoSuchAlgorithmException, SAXException {

        // we need to raise entityExpansionLimit because the dblp.xml has millions of entities
        System.setProperty("entityExpansionLimit", "10000000");

        System.out.println("building the dblp main memory DB ...");
        xmlModel = new RecordDb(dblpXmlFilename, dblpDtdFilename, false);
        System.out.format("MMDB ready: %d publs, %d pers\n\n", xmlModel.numberOfPublications(), xmlModel.numberOfPersons());

        minYear = Long.parseLong(yearFrom);
        maxYear = Long.parseLong(yearTo);

        System.out.println("Loading HDT file");
        final HDT hdt = HDTManager.loadIndexedHDT(hdtFilename, null);
        final HDTGraph graph = new HDTGraph(hdt);
        hdtModel = ModelFactory.createModelForGraph(graph);
        System.out.println("Loaded HDT file");

        // The disambiguated HDT model
        hdtPersonResource = hdtModel.getResource("http://xmlns.com/foaf/0.1/Agent");
        hdtNameProperty = hdtModel.getProperty("http://xmlns.com/foaf/0.1/name");
        hdtTitleProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/title");
        hdtIssuedProperty = hdtModel.getProperty("http://purl.org/dc/terms/issued");
        hdtTypeProperty = hdtModel.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        hdtCreatorProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/creator");
        hdtCitationProperty = hdtModel.getProperty("http://purl.org/dc/terms/references");

        // The ambiguated model
        model = ModelFactory.createDefaultModel();
        agentResource = model.createResource("http://xmlns.com/foaf/0.1/Agent");
        personResource = model.createResource("http://xmlns.com/foaf/0.1/Person");
        pubResource = model.createResource("http://xmlns.com/foaf/0.1/Document");
        typeProperty = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        idProperty = model.createProperty("http://purl.org/dc/terms/identifier");
        nameProperty = model.createProperty("http://xmlns.com/foaf/0.1/name");
        titleProperty = model.createProperty("http://purl.org/dc/elements/1.1/title");
        issuedProperty = hdtModel.createProperty("http://purl.org/dc/terms/issued");
        creatorProperty = model.createProperty("http://purl.org/dc/elements/1.1/creator");
        citationProperty = model.createProperty("http://purl.org/dc/terms/references");

    }

    /**
     * Useful for counting the nr of elements in an iterator quickly
     */
    private <T> long iteratorSize(Iterator<T> it) {
        final Iterable<T> newIterable = () -> it;
        return StreamSupport.stream(newIterable.spliterator(), true).count();
    }

    /**
     * Trims and removes all numbers from the name
     */
    private String fixName(String name) {
        return name.replaceAll("\\d","").trim();
    }

    /**
     * Creates a unique string-id based on the publication and person URI's
     */
    private String createUniqueID(String publicationURI, String personURI) {
        messageDigest.update((publicationURI + personURI).getBytes());
        return Base64.getUrlEncoder().encodeToString(messageDigest.digest()).substring(0, 44);
    }

    /**
     * Given a person the DBLP XML file, find their possible names and pick a random one
     */
    private String randomAltName(Person xmlPerson) {
        final Collection<PersonName> altNames = xmlPerson.getNames();
        int i = (int) (Math.random() * altNames.size());
        for(PersonName name: altNames) if (--i < 0) return name.name();
        throw new AssertionError();
    }

    /**
     * Returns true if the URI does not end with some number, e.g. _0001
     */
    private boolean isUriFirst(String uri) {
        return !uri.matches(".*_[\\d]{4}$");
    }

    @Override
    public void ambiguate() throws FileNotFoundException {

        ResIterator hdtPersons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

        // Find the nr of persons in the HDT file so we can show a progress bar
        long nrOfHdtPersons = iteratorSize(hdtPersons);

        // We used up the iterator above, so we create it again
        hdtPersons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

        // Consider all persons in the HDT file
        try(ProgressBar pb = new ProgressBar("Processing persons", nrOfHdtPersons, 100)) {

            // Note that we will consider some persons and publications multiple times, we rely on Jena's createResource()
            // to not create the same resource twice
            while(hdtPersons.hasNext()) {

                final Resource hdtPerson = hdtPersons.nextResource();
                final ResIterator publications = hdtModel.listResourcesWithProperty(hdtCreatorProperty, hdtPerson);

                // Find all publications of this person
                while(publications.hasNext()) {

                    final Resource hdtPublication = publications.nextResource();
                    final long issued = Long.parseLong(hdtPublication.getProperty(hdtIssuedProperty).getLiteral().getString());

                    // We will only consider this person and publication if the publication is issued during the given period
                    if(issued < minYear || issued >= maxYear) continue;

                    // Create a new unique resource in the ambiguated model every time this person is an author of a publication
                    // We mark this resource as an "agent" type, i.e. an entity that co-authored this paper
                    final String agentURI = hdtPerson.getURI() + "/" + createUniqueID(hdtPublication.getURI(), hdtPerson.getURI());
                    final Resource agent = model.createResource(agentURI, agentResource);

                    // In order to further ambiguate agents, we consult the original DBLP xml for alternative names
                    String name = hdtPerson.getProperty(hdtNameProperty).getLiteral().getString();
                    final Person xmlPerson = xmlModel.getPersonByName(name);
                    // If we can find the person in the XML data, we pick a random name
                    name = xmlPerson == null ? name : randomAltName(xmlModel.getPersonByName(name));
                    agent.addLiteral(nameProperty, fixName(name));

                    // Add a publication resource in the ambiguated model
                    final Resource publication = model.createResource(hdtPublication.getURI(), pubResource);

                    // Add the title and issued property to the publication
                    final Statement titleStatement = hdtPublication.getProperty(hdtTitleProperty);
                    if(titleStatement != null) publication.addLiteral(titleProperty, titleStatement.getLiteral());

                    publication.addLiteral(issuedProperty, issued);
                    publication.addProperty(creatorProperty, agent);

                    // Add citations of this publication too, even though they might fall outside the given year range
                    // Note that these publication resources have no author information, as if we did so,
                    // we would be adding most of the previous publications and persons
                    final StmtIterator references = hdtPublication.listProperties(hdtCitationProperty);
                    while(references.hasNext()) {
                        final Resource hdtCitation = references.nextStatement().getResource();
                        final long citationIssued = Long.parseLong(hdtCitation.getProperty(hdtIssuedProperty).getLiteral().getString());
                        final Statement citationTitle = hdtCitation.getProperty(hdtTitleProperty);
                        final Resource citation = model.createResource(hdtCitation.getURI(), pubResource);
                        citation.addLiteral(issuedProperty, citationIssued);
                        if(citationTitle != null) citation.addLiteral(titleProperty, citationTitle.getLiteral());
                        publication.addProperty(citationProperty, citation);
                    }

                    // Add the ground truth to the ambiguated model, these resources are of "person" type as they point
                    // to a real life person
                    String personURI = hdtPerson.getURI();
                    // Equalize all URI's so they all end with four digits
                    if(isUriFirst(personURI)) personURI += "_0000";
                    final Resource person = model.createResource(personURI, personResource);
                    agent.addProperty(idProperty, person);
                }
                pb.step();
            }
        }

        ResIterator persons = model.listResourcesWithProperty(typeProperty, personResource);
        long nrOfPersons = iteratorSize(persons);
        ResIterator agents = model.listResourcesWithProperty(typeProperty, agentResource);
        long nrOfAgents = iteratorSize(agents);
        ResIterator publications = model.listResourcesWithProperty(typeProperty, pubResource);
        long nrOfPublications = iteratorSize(publications);

        System.out.println("Done ambiguating, new ambiguated file will have:");
        System.out.println("Nr of persons: " + nrOfPersons + " (ground truth)");
        System.out.println("Nr of agents: " + nrOfAgents);
        System.out.println("Nr of publications: " + nrOfPublications);
        System.out.println("Writing to file");
        File outFile = new File("out/dblp-"+minYear+"-"+maxYear+".ttl");
        if(outFile.getParentFile().mkdirs()) {
            RDFDataMgr.write(new FileOutputStream(outFile), model, Lang.TURTLE);
            System.out.println("Done");
        }
        else System.err.println("Error writing output file");
    }
}

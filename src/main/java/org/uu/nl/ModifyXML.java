package org.uu.nl;

import org.apache.jena.rdf.model.*;
import org.dblp.mmdb.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Optional;

public class ModifyXML {

    static final String authorPrefix = "http://dblp.l3s.de/d2r/resource/authors/";
    static final String publicationPrefix = "http://dblp.l3s.de/d2r/resource/publications/";
    static final Property nameProperty = ResourceFactory.createProperty("http://xmlns.com/foaf/0.1/name");
    static final Property creatorProperty = ResourceFactory.createProperty("http://purl.org/dc/elements/1.1/creator");
    static final Property typeProperty = ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    static final Property titleProperty = ResourceFactory.createProperty("http://purl.org/dc/elements/1.1/title");
    //static final Property venueProperty = ResourceFactory.createProperty("http://purl.org/dc/terms/partOf");
    //static final Resource venueType = ResourceFactory.createResource("");
    static final Resource agentType = ResourceFactory.createResource("http://xmlns.com/foaf/0.1/Agent");
    static final Resource publicationType = ResourceFactory.createResource("http://xmlns.com/foaf/0.1/Document");

    public ModifyXML(String dblpXmlFilename, String dblpDtdFilename) {
        // we need to raise entityExpansionLimit because the dblp.xml has millions of entities
        System.setProperty("entityExpansionLimit", "10000000");

        System.out.println("building the dblp main memory DB ...");
        RecordDbInterface dblp;
        try {
            dblp = new RecordDb(dblpXmlFilename, dblpDtdFilename, false);
        }
        catch (final IOException ex) {
            System.err.println("cannot read dblp XML: " + ex.getMessage());
            return;
        }
        catch (final SAXException ex) {
            System.err.println("cannot parse XML: " + ex.getMessage());
            return;
        }
        System.out.format("MMDB ready: %d publs, %d pers\n\n", dblp.numberOfPublications(), dblp.numberOfPersons());

        Model rdfModel = ModelFactory.createDefaultModel();

        for(Person person : dblp.getPersons()) {

            for (Publication publication : person.getPublications()) {

                if(publication.getYear() < 2018) continue;

                final Resource publicationResource = rdfModel.createResource(publicationPrefix + publication.getKey());
                final Resource personResource = rdfModel.createResource(authorPrefix + person.getPid());

                publication.getFields().forEach(System.out::println);

                final Optional<Field> optionalTitle = publication.getFields("title").stream().findFirst();
                optionalTitle.ifPresent(field -> publicationResource.addLiteral(titleProperty, field.value()));

                for(PersonName name : person.getNames()) personResource.addLiteral(nameProperty, name.name().replaceAll("\\d",""));
                personResource.addProperty(typeProperty, agentType);
                publicationResource.addProperty(creatorProperty, personResource);
                publicationResource.addProperty(typeProperty, publicationType);
            }
        }

        rdfModel.size();
        ResIterator personIterator = rdfModel.listResourcesWithProperty(typeProperty, agentType);
        ResIterator publicationIterator = rdfModel.listResourcesWithProperty(typeProperty, publicationType);

        int personCount = 0;
        while (personIterator.hasNext()) {
            Resource person = personIterator.nextResource();
            StmtIterator it = rdfModel.listStatements(person, nameProperty, (String) null);

            while (it.hasNext()) {
                System.out.println(it.nextStatement().getLiteral().getString());
            }
            System.out.println();
            //person.getProperty(nameProperty);
            personCount++;
        }

        int publicationCount = 0;
        while (publicationIterator.hasNext()) {
            Resource publication = publicationIterator.nextResource();
            publicationCount++;
        }

        System.out.println(personCount);
        System.out.println(publicationCount);
    }
}

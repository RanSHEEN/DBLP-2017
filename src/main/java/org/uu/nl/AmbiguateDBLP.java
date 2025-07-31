package org.uu.nl;

import me.tongfei.progressbar.ProgressBar;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Factory;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
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
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class AmbiguateDBLP implements Ambiguator {

    private final Dataset dataset;
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
    private final double noisePercentage;

    public AmbiguateDBLP(
        String hdtFilename, 
        String dblpXmlFilename, 
        String dblpDtdFilename, 
        String yearFrom, 
        String yearTo,
        String noisePct
        ) throws IOException, NoSuchAlgorithmException, SAXException {

        // we need to raise entityExpansionLimit because the dblp.xml has millions of entities
        System.setProperty("entityExpansionLimit", "10000000");

        System.out.println("building the dblp main memory DB ...");
        xmlModel = new RecordDb(dblpXmlFilename, dblpDtdFilename, false);
        System.out.format("MMDB ready: %d publs, %d pers\n\n", xmlModel.numberOfPublications(), xmlModel.numberOfPersons());

        minYear = Long.parseLong(yearFrom);
        maxYear = Long.parseLong(yearTo);
        // parse noise percentage (0-100) to a [0,1] fraction
        this.noisePercentage = Math.max(0.0, Math.min(1.0, Double.parseDouble(noisePct) / 100.0));

        System.out.println("Loading HDT file");
        // final HDT hdt = HDTManager.loadIndexedHDT(hdtFilename, null);
        // final HDTGraph graph = new HDTGraph(hdt);
        // hdtModel = ModelFactory.createModelForGraph(graph);
        // Replace the above with the following line to load the NT file
        // if (hdtFilename.endsWith(".hdt")) {
        //     throw new UnsupportedOperationException("HDT not supported in streaming mode");
        // } else if (hdtFilename.endsWith(".nt")) {
        //     Graph graph = Factory.createDefaultGraph();
        //     StreamRDF stream = StreamRDFLib.graph(graph);
        //     try (InputStream in = new FileInputStream(hdtFilename)) {
        //         RDFDataMgr.parse(stream, in, Lang.NTRIPLES);
        //         hdtModel = ModelFactory.createModelForGraph(graph);
        //     } catch (IOException e) {
        //         throw new RuntimeException("Error reading NT file", e);
        //     }
        // } else {
        //     throw new IllegalArgumentException("Unsupported RDF file format: " + hdtFilename);
        // }

        // if (hdtModel == null) {
        //     throw new IllegalStateException("hdtModel could not be initialized");
        // }
        
        String tdbPath = "/home/xinran/dblp-tdb";
        File tdbDir = new File(tdbPath);
        if (!tdbDir.exists()) {
            tdbDir.mkdirs();  // Create the directory if it does not exist
        }
        this.dataset = TDB2Factory.connectDataset(tdbPath);
        hdtModel = dataset.getDefaultModel();

        // if (hdtFilename.endsWith(".hdt")) {
        //     throw new UnsupportedOperationException("HDT not supported in streaming mode");
        // } else if (hdtFilename.endsWith(".nt")) {
        //     // 安全写入 .nt 文件到 TDB2 数据集中
        //     dataset.begin(ReadWrite.WRITE);
        //     try {
        //         // Model model = dataset.getDefaultModel();
        //         RDFDataMgr.read(hdtModel, hdtFilename); // Read the NT file into the model
        //         dataset.commit();
        //     } catch (Exception e) {
        //         dataset.abort(); // Rollback on error
        //         throw e;
        //     } finally {
        //         if (dataset.isInTransaction()) {
        //             dataset.end(); // Ensure the transaction is ended
        //         }
        //     }
        // }

        if (hdtFilename.endsWith(".hdt")) {
            throw new UnsupportedOperationException("HDT not supported in streaming mode");
        } else if (hdtFilename.endsWith(".nt")) {
            // 安全写入 .nt 文件到 TDB2 数据集中
            dataset.begin(ReadWrite.WRITE);
            try {
                RDFDataMgr.read(dataset.getDefaultModel(), hdtFilename);  // 加载 .nt 数据
                dataset.commit();  // ✅ 提交写事务
            } catch (Exception e) {
                dataset.abort();  // ❗ 出错时回滚事务
                throw new RuntimeException("Error reading .nt file", e);
            } finally {
                if (dataset.isInTransaction()) {
                    dataset.end();  // ✅ 无论成功与否都必须结束事务
                }
            }
        }

        System.out.println("Loaded NT/HDT file");

        // The disambiguated HDT model
        // hdtPersonResource = hdtModel.getResource("http://xmlns.com/foaf/0.1/Agent");
        // hdtNameProperty = hdtModel.getProperty("http://xmlns.com/foaf/0.1/name");
        // hdtTitleProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/title");
        // hdtIssuedProperty = hdtModel.getProperty("http://purl.org/dc/terms/issued");
        // hdtTypeProperty = hdtModel.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        // hdtCreatorProperty = hdtModel.getProperty("http://purl.org/dc/elements/1.1/creator");
        // hdtCitationProperty = hdtModel.getProperty("http://purl.org/dc/terms/references");

        // // RDF type 保持不变
        // hdtTypeProperty     = hdtModel.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        // // Core property binding (based on dblp schema)
        // hdtNameProperty     = hdtModel.getProperty("https://dblp.org/rdf/schema#signatureDblpName");
        // hdtTitleProperty    = hdtModel.getProperty("https://dblp.org/rdf/schema#title");
        // hdtIssuedProperty   = hdtModel.getProperty("https://dblp.org/rdf/schema#yearOfPublication");
        // hdtCreatorProperty  = hdtModel.getProperty("https://dblp.org/rdf/schema#authoredBy");
        // // 你的数据中没有明显 references 属性，可留空或移除
        // // hdtCitationProperty = hdtModel.getProperty("https://dblp.org/rdf/schema#references"); // 如果未来找到可加

        // // The HDT model uses the FOAF vocabulary for persons, so we need to bind the FOAF resources
        // // Resource type: used in listResourcesWithProperty(type, ...)
        // hdtPersonResource = hdtModel.getResource("https://dblp.org/rdf/schema#AuthorSignature");

        // agentResource     = hdtModel.createResource("https://dblp.org/rdf/schema#AuthorSignature");
        // personResource    = hdtModel.createResource("https://dblp.org/rdf/schema#Person");
        // pubResource       = hdtModel.createResource("https://dblp.org/rdf/schema#Publication");

        // // The ambiguated model
        // model = ModelFactory.createDefaultModel();
        // agentResource = model.createResource("http://xmlns.com/foaf/0.1/Agent");
        // personResource = model.createResource("http://xmlns.com/foaf/0.1/Person");
        // pubResource = model.createResource("http://xmlns.com/foaf/0.1/Document");
        // typeProperty = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        // idProperty = model.createProperty("http://purl.org/dc/terms/identifier");
        // nameProperty = model.createProperty("http://xmlns.com/foaf/0.1/name");
        // titleProperty = model.createProperty("http://purl.org/dc/elements/1.1/title");
        // issuedProperty = hdtModel.createProperty("http://purl.org/dc/terms/issued");
        // creatorProperty = model.createProperty("http://purl.org/dc/elements/1.1/creator");
        // citationProperty = model.createProperty("http://purl.org/dc/terms/references");

        // The disambiguated HDT model
        this.hdtPersonResource    = hdtModel.getResource("https://dblp.org/rdf/schema#AuthorSignature");
        this.hdtNameProperty      = hdtModel.getProperty("https://dblp.org/rdf/schema#signatureDblpName");
        this.hdtTitleProperty     = hdtModel.getProperty("https://dblp.org/rdf/schema#title");
        this.hdtIssuedProperty    = hdtModel.getProperty("https://dblp.org/rdf/schema#yearOfPublication");
        this.hdtTypeProperty      = hdtModel.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        this.hdtCreatorProperty   = hdtModel.getProperty("https://dblp.org/rdf/schema#authoredBy");
        this.hdtCitationProperty  = hdtModel.getProperty("https://dblp.org/rdf/schema#dummyCitation"); // 如果你确定没引用，可删除后续引用处理逻辑

        // The ambiguated model
        this.model           = ModelFactory.createDefaultModel();
        this.agentResource   = model.createResource("https://dblp.org/rdf/schema#AuthorSignature");
        this.personResource  = model.createResource("https://dblp.org/rdf/schema#Person");
        this.pubResource     = model.createResource("https://dblp.org/rdf/schema#Publication");
        this.typeProperty    = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        this.idProperty      = model.createProperty("http://purl.org/dc/terms/identifier");
        this.nameProperty    = model.createProperty("https://dblp.org/rdf/schema#signatureDblpName");
        this.titleProperty   = model.createProperty("https://dblp.org/rdf/schema#title");
        this.issuedProperty  = hdtModel.createProperty("https://dblp.org/rdf/schema#yearOfPublication");
        this.creatorProperty = model.createProperty("https://dblp.org/rdf/schema#authoredBy");
        this.citationProperty= model.createProperty("https://dblp.org/rdf/schema#dummyCitation"); // 与上保持一致，或删掉
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

    // // 遍历 Person → 找 Publication
    // @Override
    // public void ambiguate() throws FileNotFoundException {
    //     dataset.begin(ReadWrite.READ); 

    //     try {
    //         ResIterator hdtPersons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

    //         // Find the nr of persons in the HDT file so we can show a progress bar
    //         long nrOfHdtPersons = iteratorSize(hdtPersons);

    //         // We used up the iterator above, so we create it again
    //         hdtPersons = hdtModel.listResourcesWithProperty(hdtTypeProperty, hdtPersonResource);

    //         // Consider all persons in the HDT file
    //         try(ProgressBar pb = new ProgressBar("Processing persons", nrOfHdtPersons, 100)) {

    //             // Note that we will consider some persons and publications multiple times, we rely on Jena's createResource()
    //             // to not create the same resource twice
    //             while(hdtPersons.hasNext()) {

    //                 final Resource hdtPerson = hdtPersons.nextResource();
    //                 final ResIterator publications = hdtModel.listResourcesWithProperty(hdtCreatorProperty, hdtPerson);

    //                 // Find all publications of this person
    //                 while(publications.hasNext()) {

    //                     final Resource hdtPublication = publications.nextResource();
    //                     final long issued = Long.parseLong(hdtPublication.getProperty(hdtIssuedProperty).getLiteral().getString());

    //                     // We will only consider this person and publication if the publication is issued during the given period
    //                     if(issued < minYear || issued >= maxYear) continue;

    //                     // Create a new unique resource in the ambiguated model every time this person is an author of a publication
    //                     // We mark this resource as an "agent" type, i.e. an entity that (co-)authored this paper
    //                     final String agentURI = hdtPerson.getURI() + "/" + createUniqueID(hdtPublication.getURI(), hdtPerson.getURI());
    //                     final Resource agent = model.createResource(agentURI, agentResource);

    //                     // In order to further ambiguate agents, we consult the original DBLP xml for alternative names
    //                     String name = hdtPerson.getProperty(hdtNameProperty).getLiteral().getString();
    //                     final Person xmlPerson = xmlModel.getPersonByName(name);

    //                     // If we can find the person in the XML data, we pick a random name
    //                     // name = xmlPerson == null ? name : randomAltName(xmlModel.getPersonByName(name));
    //                     // Introduce noise: with probability noisePercentage, pick a random alt name
    //                     if (xmlPerson != null && Math.random() < noisePercentage) {
    //                         name = randomAltName(xmlPerson);
    //                     }

    //                     agent.addLiteral(nameProperty, fixName(name));

    //                     // Add a publication resource in the ambiguated model
    //                     final Resource publication = model.createResource(hdtPublication.getURI(), pubResource);

    //                     // Add the title and issued property to the publication
    //                     final Statement titleStatement = hdtPublication.getProperty(hdtTitleProperty);
    //                     if(titleStatement != null) publication.addLiteral(titleProperty, titleStatement.getLiteral());

    //                     publication.addLiteral(issuedProperty, issued);
    //                     publication.addProperty(creatorProperty, agent);

    //                     // Add citations of this publication too, even though they might fall outside the given year range
    //                     // Note that these publication resources have no author information, as if we did so,
    //                     // we would be adding most of the previous publications and persons
    //                     final StmtIterator references = hdtPublication.listProperties(hdtCitationProperty);
    //                     while(references.hasNext()) {
    //                         final Resource hdtCitation = references.nextStatement().getResource();
    //                         final long citationIssued = Long.parseLong(hdtCitation.getProperty(hdtIssuedProperty).getLiteral().getString());
    //                         final Statement citationTitle = hdtCitation.getProperty(hdtTitleProperty);
    //                         final Resource citation = model.createResource(hdtCitation.getURI(), pubResource);
    //                         citation.addLiteral(issuedProperty, citationIssued);
    //                         if(citationTitle != null) citation.addLiteral(titleProperty, citationTitle.getLiteral());
    //                         publication.addProperty(citationProperty, citation);
    //                     }

    //                     // Add the ground truth to the ambiguated model, these resources are of "person" type as they point
    //                     // to a real life person
    //                     String personURI = hdtPerson.getURI();
    //                     // Equalize all URI's so they all end with four digits
    //                     if(isUriFirst(personURI)) personURI += "_0000";
    //                     final Resource person = model.createResource(personURI, personResource);
    //                     agent.addProperty(idProperty, person);
    //                 }
    //                 pb.step();
    //             }
    //         }

    // 遍历 Publication → 找作者
    @Override
    public void ambiguate() throws FileNotFoundException {

        dataset.begin(ReadWrite.READ);

        try {
            // 遍历所有 Publication 实体
            ResIterator publications = hdtModel.listResourcesWithProperty(
                    hdtTypeProperty,
                    hdtModel.getResource("https://dblp.org/rdf/schema#Publication")
            );

            long totalPublications = iteratorSize(publications);
            publications = hdtModel.listResourcesWithProperty(
                    hdtTypeProperty,
                    hdtModel.getResource("https://dblp.org/rdf/schema#Publication")
            );

            try (ProgressBar pb = new ProgressBar("Processing publications", totalPublications, 100)) {

                while (publications.hasNext()) {
                    final Resource hdtPublication = publications.nextResource();

                    // 过滤年份
                    Statement issuedStmt = hdtPublication.getProperty(hdtIssuedProperty);
                    if (issuedStmt == null) {
                        pb.step();
                        continue;
                    }
                    long issued = Long.parseLong(issuedStmt.getLiteral().getString());
                    if (issued < minYear || issued >= maxYear) {
                        pb.step();
                        continue;
                    }

                    // 遍历所有作者（Person 或 AuthorSignature）
                    StmtIterator authors = hdtPublication.listProperties(hdtCreatorProperty);
                    while (authors.hasNext()) {
                        Resource hdtAuthor = authors.nextStatement().getResource();

                        // 创建模糊 Agent
                        final String agentURI = hdtAuthor.getURI() + "/" +
                                createUniqueID(hdtPublication.getURI(), hdtAuthor.getURI());
                        final Resource agent = model.createResource(agentURI, agentResource);

                        // 获取作者姓名
                        String name = "";
                        Statement nameStmt = hdtAuthor.getProperty(hdtNameProperty);
                        if (nameStmt != null) {
                            name = nameStmt.getLiteral().getString();
                        }
                        final Person xmlPerson = xmlModel.getPersonByName(name);
                        if (xmlPerson != null && Math.random() < noisePercentage) {
                            name = randomAltName(xmlPerson);
                        }
                        agent.addLiteral(nameProperty, fixName(name));

                        // 创建 Publication 资源
                        final Resource publication = model.createResource(hdtPublication.getURI(), pubResource);

                        // 添加标题和年份
                        Statement titleStatement = hdtPublication.getProperty(hdtTitleProperty);
                        if (titleStatement != null) {
                            publication.addLiteral(titleProperty, titleStatement.getLiteral());
                        }
                        publication.addLiteral(issuedProperty, issued);
                        publication.addProperty(creatorProperty, agent);

                        // ground truth Person
                        String personURI = hdtAuthor.getURI();
                        if (isUriFirst(personURI)) personURI += "_0000";
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
            ResIterator pubs = model.listResourcesWithProperty(typeProperty, pubResource);
            long nrOfPublications = iteratorSize(pubs);

            System.out.println("Done, new ambiguated file will have:");
            System.out.println("Nr of persons: " + nrOfPersons + " (ground truth)");
            System.out.println("Nr of agents: " + nrOfAgents);
            System.out.println("Nr of publications: " + nrOfPublications);
            System.out.println("Writing to file");
            File outFile = new File("dblp-"+minYear+"-"+maxYear+".ttl");
            RDFDataMgr.write(new FileOutputStream(outFile), model, Lang.TURTLE);
            System.out.println("Done");
            // dataset.commit();  
            // dataset.end(); 
        } catch (Exception e) {
            dataset.abort();  
            throw e;
        } finally {
            if (dataset.isInTransaction()) {
                dataset.end();
            }
        }
    }
}

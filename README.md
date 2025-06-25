This code was written for ambiguating the DBLP 2017 datadump which can be found here: http://www.rdfhdt.org/datasets/

It requires both the HDT dump and the DBLP XML/DTD files found here: https://dblp.org/xml/release/

Because the HDT is from 2017, it is recommended to take an XML dump and associated DTD from 2017 or later

To compile the code into a jar, run: mvn clean compile assembly:single

To run the jar: java -Xmx16g -jar dblp.jar dblp-2017-01-24.hdt dblp-2017-01-01.xml dblp-2016-10-01.dtd 2014 2017

First argument is the location of the HDT file
Second argument is the location of the XML file
Third argument is the location of the DTD file
Fourth argument is the starting year (including) of the period you wish to ambiguate
Fifth argument is the ending year (excluding) of the period you wish to ambiguate

The output file will be written to out/... .ttl

Extracting the ground truth can be done by exporting the result of this SPARQL query to CSV:

PREFIX dcterm: <http://purl.org/dc/terms/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person (GROUP_CONCAT(?author;SEPARATOR=",") AS ?authors)
WHERE {
  ?person a foaf:Person .
  ?author dcterm:identifier ?person .
} GROUP BY ?person

Afterwards you can for example modify the result in R to fit a particular supervised disambiguation algorithm:

library(data.table)
library(stringr)

dblp <- fread(file = "~/Downloads/dblp_authors.csv", select = c(1:2), header = T)
dblp <- dblp[, .(author = unlist(strsplit(authors, ",", fixed = T))), by = person]
dblp <- dblp[, .(author, id = as.numeric(substr(person, str_length(person) - 3, str_length(person))), person = substr(person, 0, str_length(person) - 5))]


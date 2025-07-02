This project is a debugged version of Jurian's dblp tool: https://github.com/Jurian/dblp

# DBLP Ambiguation Tool

This project provides a Java-based tool for performing author disambiguation on the DBLP 2017 datadump using HDT and XML/DTD inputs.

---

## Table of Contents

* [Overview](#overview)
* [Prerequisites](#prerequisites)
* [Data Sources](#data-sources)
* [Installation](#installation)
* [Usage](#usage)
* [Ground Truth Extraction](#ground-truth-extraction)
* [Example Workflow](#example-workflow)
* [Update](#update)
* [License](#license)

---

## Overview

The ambiguity of author names in bibliographic datasets can hinder accurate analysis. This tool:

1. Loads the DBLP 2017 HDT dump
2. Parses the associated XML and DTD files
3. Extracts and clusters author entities by publication period
4. Outputs a clean Turtle (`.ttl`) file containing disambiguated author records

---

## Prerequisites

* **Java 8+** (with at least 16 GB heap for large datasets)
* **Maven 3.6+**
* **R** (optional, for downstream analysis and ground truth processing)

---

## Data Sources

1. **HDT Dump (2017):** [rdfhdt.org/datasets](http://www.rdfhdt.org/datasets/)
2. **DBLP XML & DTD:** [dblp.org/xml/release](https://dblp.org/xml/release/)

> **Note:** Use XML and DTD files from 2017 or later to match the HDT version.

---

## Installation

Clone this repository and build the `jar`:

```
git clone https://github.com/RanSHEEN/DBLP-2017.git
cd DBLP-2017
mvn clean compile assembly:single
```

This produces `target/dblp-ambig.jar`.

---

## Usage

Run the jar with the following arguments:

```
java -Xmx16g -jar target/dblp-ambig.jar \
  <HDT_FILE> <XML_FILE> <DTD_FILE> <START_YEAR> <END_YEAR> <NOISE_PCT>
```

| Argument       | Description                                                           |
| -------------- | --------------------------------------------------------------------- |
| `<HDT_FILE>`   | Path to the DBLP HDT dump (e.g., `dblp-2017-01-24.hdt`)               |
| `<XML_FILE>`   | Path to the DBLP XML dump (e.g., `dblp-2017-01-01.xml`)               |
| `<DTD_FILE>`   | Path to the DTD file (e.g., `dblp-2016-10-01.dtd`)                    |
| `<START_YEAR>` | Inclusive start of the publication period to ambiguate (e.g., `2014`) |
| `<END_YEAR>`   | Exclusive end of the period (e.g., `2017`)                            |

The output Turtle files are written under the `out/` directory.

---

## Ground Truth Extraction

To generate a CSV of true author–person mappings, run this SPARQL query against your output dataset and export as CSV:

```sparql
PREFIX dcterm: <http://purl.org/dc/terms/>
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>

SELECT ?person (GROUP_CONCAT(?author; SEPARATOR=",") AS ?authors)
WHERE {
  ?person a foaf:Person .
  ?author dcterm:identifier ?person .
}
GROUP BY ?person
```

You can then process the CSV in R:

```r
library(data.table)
library(stringr)

dblp <- fread("~/Downloads/dblp_authors.csv", select = 1:2)

# Split concatenated authors into rows
dblp <- dblp[, .(
  author = unlist(strsplit(authors, ",", fixed = TRUE))
), by = person]

# Extract numeric ID and base URI
dblp <- dblp[, .(
  author,
  id = as.integer(substr(person, str_length(person) - 3, str_length(person))),
  person = substr(person, 1, str_length(person) - 5)
)]
```

---

## Example Workflow

```
# 1. Build
mvn clean compile assembly:single

# 2. Run
./run.sh dblp-2017-01-24.hdt dblp-2017-01-01.xml dblp-2016-10-01.dtd 2014 2017 20

# 3. Extract truth
# (use SPARQL and R as shown above)
```

---

## Update

* **Build without assembly**:

  ```
  mvn clean compile
  ```
* **Run via helper script**:

  ```
  ./run.sh <HDT> <XML> <DTD> <start> <end> <noise_pct>
  ```
  If you have the documents : 
  ```
  ./run.sh dblp-20170124.hdt dblp-2017-01-01.xml dblp.dtd 2014 2017 20
  ```


---

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

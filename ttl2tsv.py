#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ttl2tsv.py
- Pipeline: Turtle (.ttl) -> N-Triples (.nt) -> TSV (.tsv) -> Clean TSV
- Combines functionalities of ttl2nt, nt2tsv, and clean tsv
- Usage: python ttl2tsv.py input.ttl output.tsv
"""

import sys
import os
from rdflib import Graph, Literal
from tqdm import tqdm

def shorten(uri: str) -> str:
    """Return last part of URI or literal."""
    if uri.startswith('http'):
        if '#' in uri:
            return uri.split('#')[-1]
        elif '/' in uri:
            return uri.rstrip('/').split('/')[-1]
    return uri

def ttl_to_nt(input_ttl: str, output_nt: str):
    print(f"Parsing Turtle file: {input_ttl}")
    g = Graph()
    g.parse(input_ttl, format="turtle")
    g.serialize(output_nt, format="nt")
    print(f"✅ Converted TTL to NT: {output_nt}")

def nt_to_tsv(input_nt: str, output_tsv: str):
    print(f"Parsing N-Triples file: {input_nt}")
    g = Graph()
    g.parse(input_nt, format="nt")

    print(f"Writing TSV to: {output_tsv}")
    with open(output_tsv, "w", encoding="utf-8") as fout:
        fout.write("subject\tpredicate\tobject\n")
        for s, p, o in tqdm(g, desc="Converting triples"):
            s_str = shorten(str(s))
            p_str = shorten(str(p))

            if isinstance(o, Literal):
                if o.datatype:
                    o_str = f"{o.value} ({shorten(str(o.datatype))})"
                else:
                    o_str = str(o)
            else:
                o_str = shorten(str(o))

            fout.write(f"{s_str}\t{p_str}\t{o_str}\n")
    print(f"✅ Conversion completed: {output_tsv}")

def clean_tsv(input_tsv: str, output_clean_tsv: str):
    print(f"Cleaning TSV file: {input_tsv}")
    with open(input_tsv, "r", encoding="utf-8", errors="ignore") as fin, \
         open(output_clean_tsv, "w", encoding="utf-8") as fout:
        for line in fin:
            cleaned = line.replace('"', '')
            fout.write(cleaned)
    print(f"✅ Cleaned TSV saved: {output_clean_tsv}")

def ttl_to_tsv(input_ttl: str, final_output_tsv: str):
    temp_nt = os.path.splitext(final_output_tsv)[0] + ".nt"
    temp_tsv = os.path.splitext(final_output_tsv)[0] + "_raw.tsv"

    ttl_to_nt(input_ttl, temp_nt)
    nt_to_tsv(temp_nt, temp_tsv)
    clean_tsv(temp_tsv, final_output_tsv)

    # Optionally remove intermediate files
    if os.path.exists(temp_nt):
        os.remove(temp_nt)
    if os.path.exists(temp_tsv):
        os.remove(temp_tsv)
    print("🎉 All steps completed successfully!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_all.py input.ttl output.tsv")
        sys.exit(1)

    input_ttl = sys.argv[1]
    final_output_tsv = sys.argv[2]
    ttl_to_tsv(input_ttl, final_output_tsv)

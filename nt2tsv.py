#!/usr/bin/env python3
import re

# 匹配 N-Triples 的三元组行： <s> <p> o .
PAT = re.compile(r'^(?P<s>\S+)\s+(?P<p>\S+)\s+(?P<o>.+?)\s+\.\s*$')

in_nt  = "dblp-2014-2017-20noisePct.nt"
out_tsv = "triples-clean.tsv"

with open(in_nt, encoding="utf-8") as fin, \
     open(out_tsv, "w", encoding="utf-8") as fout:

    # 写表头
    fout.write("subject\tpredicate\tobject\n")

    for line in fin:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = PAT.match(line)
        if not m:
            continue

        s = m.group("s")
        p = m.group("p")
        o = m.group("o")

        # 如果是字面量，去掉起始和结束的那一对引号
        if o.startswith('"'):
            # 找第一个未被转义的结束引号
            end = None
            for i in range(1, len(o)):
                if o[i] == '"' and o[i-1] != '\\':
                    end = i
                    break
            if end is not None:
                # literal content + 后缀（如 @en 或 ^^<datatype>）
                content = o[1:end]
                suffix  = o[end+1:]
                o = content + suffix

        # 写入一行，纯 \t 分隔，不做任何字段包裹
        fout.write(f"{s}\t{p}\t{o}\n")

print(f"Wrote cleaned TSV to {out_tsv}")

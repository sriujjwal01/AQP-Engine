import re
from dataclasses import dataclass
from typing import Optional, List

AGG_RE = r"(?P<agg>COUNT\(\*\)|COUNT\((?P<count_col>[^)]+)\)|SUM\((?P<sum_col>[^)]+)\)|AVG\((?P<avg_col>[^)]+)\))"

@dataclass
class ParsedQuery:
    select_cols: List[str]  
    agg: str
    agg_col: Optional[str]
    source: str
    where_col: Optional[str]
    where_op: Optional[str]
    where_val: Optional[str]
    group_by: List[str]

def parse(sql: str) -> ParsedQuery:
   
    s = re.sub(r"\s+", " ", sql.strip())
    
    m = re.match(rf"SELECT (?P<select>.+?) FROM (?P<src>[^ ]+)(?: WHERE (?P<wcol>[^ ]+) (?P<wop>=|!=|>|<|>=|<=) (?P<wval>[^ ]+))?(?: GROUP BY (?P<gby>.+))?;?\Z", s, re.IGNORECASE)
    if not m:
        raise ValueError("Unsupported SQL. Examples: SELECT COUNT(*) FROM file.csv; SELECT city, SUM(amount) FROM file.csv GROUP BY city")
    select = m.group('select').strip()
    src = m.group('src').strip()
    wcol = m.group('wcol')
    wop  = m.group('wop')
    wval = m.group('wval')
    gby  = m.group('gby')
    group_by = [c.strip() for c in gby.split(',')] if gby else []

    
    parts = [p.strip() for p in select.split(',')]
  
    agg_part = parts[-1]
    am = re.match(AGG_RE, agg_part, re.IGNORECASE)
    if not am:
        raise ValueError("SELECT must end with an aggregate like COUNT(*), SUM(x), AVG(x)")
    agg = am.group('agg').upper()
    agg_col = am.group('count_col') or am.group('sum_col') or am.group('avg_col')
    if agg_col:
        agg_col = agg_col.strip()

    select_cols = [p for p in parts[:-1]]

   
    if group_by and [c.lower() for c in group_by] != [c.lower() for c in select_cols]:
        raise ValueError("GROUP BY columns must match the non-aggregate SELECT columns in order.")

    return ParsedQuery(
        select_cols=select_cols,
        agg=agg,
        agg_col=agg_col,
        source=src,
        where_col=wcol,
        where_op=wop,
        where_val=wval,
        group_by=group_by
    )

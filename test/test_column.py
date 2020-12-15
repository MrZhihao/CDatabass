import random
import pandas as pd
import numpy as np
from .conftest import *
from databass import *
from databass.tables import InMemoryTable

simple_qs = [
    "SELECT * FROM data WHERE a = 1000",
    "SELECT d1.a, d2.b FROM data AS d1, data AS d2",
    "SELECT d1.a, d2.b FROM data AS d1, data AS d2 WHERE d1.a=d2.a",
    "SELECT d1.a, d2.b FROM data AS d1, (SELECT * FROM data AS d1) AS d2 WHERE d1.a=d2.a",
    "SELECT a, a+b FROM data AS d1",
    "SELECT b FROM data AS d1 WHERE a <> 5",
    "SELECT lower(a), sum(b) FROM data GROUP BY a HAVING a = 1",
    "SELECT * from data ORDER BY a asc, b+1 desc limit 10 offset 3",
    "SELECT count(1), sum(3) FROM data GROUP BY b",
    "SELECT a+b FROM data ORDER BY a",
    "SELECT * from data ORDER BY a asc",
    "SELECT * from data ORDER BY a",
    "SELECT * from data ORDER BY a desc",
    "SELECT * from data ORDER BY a, b",
    "SELECT * from data ORDER BY a asc, b asc",
    "SELECT * from data ORDER BY a asc, b desc",
    "SELECT * from data ORDER BY a desc, b asc",
    "SELECT * from data ORDER BY a desc, b desc",
    "SELECT * from data ORDER BY a+1 desc, b desc",
    "SELECT * from data ORDER BY a+1 desc, b+2 desc",
    "SELECT * from data ORDER BY -a, -b",
    "SELECT * from data ORDER BY -a, -b desc",
    "SELECT * from data ORDER BY -a desc, -b",
    "SELECT * FROM data LIMIT 0",
    "SELECT * FROM data LIMIT 2-2",
    "SELECT * FROM data LIMIT 0+0",
    "SELECT * FROM data LIMIT 2",
    "SELECT * FROM data LIMIT 1+1",
    "SELECT * FROM data LIMIT (2*2)-2",
    "SELECT * FROM (SELECT * FROM data)",
    "SELECT * FROM (SELECT a+b, a FROM data as d1) as d2",
    "SELECT * FROM (SELECT a+b, a FROM data as d1) as d2 WHERE d2.a > 1",
    "SELECT * FROM (SELECT a+b, a FROM data as d1 WHERE d1.a > 1) as d2 WHERE d2.a > 1",
    "SELECT * FROM (SELECT a+b, a FROM data as d1 WHERE d1.a > 2) as d2 WHERE d2.a > 1",
    "SELECT 1 FROM data GROUP BY a",
    "SELECT a, sum(b) FROM data GROUP BY a HAVING a = 1",
    "SELECT sum(b) FROM data GROUP BY a HAVING a = 1",
    "SELECT a FROM data GROUP BY a",
    "SELECT sum(b) FROM data GROUP BY a",
    "SELECT sum(a) FROM data GROUP BY a",
    "SELECT sum(b) FROM data GROUP BY a HAVING 1 = 1",
    "SELECT a, sum(b) FROM data GROUP BY a HAVING a = a",
    "SELECT a, sum(b) FROM data GROUP BY a HAVING sum(b) > 2",
    """SELECT d2.x
      FROM (SELECT a AS x, sum(b) AS z
            FROM data GROUP BY a) AS d2,
          (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z = d3.y ORDER BY x""",
    """SELECT d2.x
      FROM (SELECT a AS x, count(b) AS z
            FROM data GROUP BY a) AS d2,
          (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z <> d3.y ORDER BY x""",
    """SELECT d2.x+d3.y
      FROM (SELECT a AS x, count(b) AS z
            FROM data GROUP BY a) AS d2,
          (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z = d3.y ORDER BY x, d2.z, d3.z""",
]

tpc_h_qs = [
  # """SELECT O_CUSTKEY, sum(O_TOTALPRICE) FROM ORDERS WHERE O_ORDERSTATUS = 'O' GROUP BY O_CUSTKEY """,
  # """
  # SELECT sum(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) as revenue
  # FROM LINEITEM, PART
  # WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#12' AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10 AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
  # """,
  """SELECT L_SHIPDATE, count(1) FROM LINEITEM WHERE L_SHIPDATE >= date('1999-02-02') GROUP BY L_SHIPDATE"""
]

cstore_queries = [
  # Q1
  """SELECT L_SHIPDATE, count(1) FROM LINEITEM WHERE L_SHIPDATE > date('1995-10-15') GROUP BY L_SHIPDATE""",
  # Q2
  """SELECT L_SUPPKEY, count(1) FROM LINEITEM WHERE L_SHIPDATE = date('1995-10-15') GROUP BY L_SUPPKEY""",
  # Q3
  """ SELECT L_SUPPKEY, count(1) FROM LINEITEM WHERE L_SHIPDATE > date('1995-10-15') GROUP BY L_SUPPKEY""",
  # Q4
  """SELECT O_ORDERDATE, count(L_SHIPDATE) FROM LINEITEM, ORDERS WHERE L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE > date('1995-10-15') GROUP BY O_ORDERDATE""",
  # Q5
  """SELECT L_SUPPKEY, count(L_SHIPDATE) FROM LINEITEM, ORDERS WHERE L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE = date('1995-10-15') GROUP BY L_SUPPKEY""",
  # Q6
  """SELECT L_SUPPKEY, count(L_SHIPDATE) FROM LINEITEM, ORDERS WHERE L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE > date('1995-10-15') GROUP BY L_SUPPKEY""",
  # Q7
  """SELECT C_NATIONKEY, sum(L_EXTENDEDPRICE) FROM LINEITEM, ORDERS, CUSTOMER 
  WHERE L_ORDERKEY = O_ORDERKEY AND O_CUSTKEY = C_CUSTKEY AND L_RETURNFLAG = 'R' GROUP BY C_NATIONKEY"""
]


# run this to test: pytest test/test_column.py -k "test_q" -s --disable-warnings
@pytest.mark.parametrize("q", cstore_queries)
@pytest.mark.usefixtures('context')
def test_q(context, q):
  run_query(context, q)
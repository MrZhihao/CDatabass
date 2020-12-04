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

# run this to test: pytest test/test_column.py -k "test_q" -s --disable-warnings
@pytest.mark.parametrize("q", simple_qs)
@pytest.mark.usefixtures('context')
def test_q(context, q):
  run_query(context, q)
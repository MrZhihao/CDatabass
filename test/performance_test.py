import time
import timeit
from databass import *
from databass.ops import *
from databass.exprutil import *

queries = [
  """SELECT O_CUSTKEY, sum(O_TOTALPRICE) FROM ORDERS WHERE O_ORDERSTATUS = 'O' GROUP BY O_CUSTKEY """,
  # 2.163224281044677
  """SELECT C_NATIONKEY, sum(L_EXTENDEDPRICE) FROM LINEITEM, ORDERS, CUSTOMER WHERE L_ORDERKEY=O_ORDERKEY AND O_CUSTKEY=C_CUSTKEY AND L_RETURNFLAG='R' GROUP BY C_NATIONKEY""",
  # 3.0714264559792355
  """
  SELECT sum(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) as revenue
  FROM LINEITEM, PART
  WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#12' AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10 AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
  """,
  # 1.4885033059399575
]

def performance_record(qs, db):
  opt = Optimizer()
  data = []
  for q in qs:
    plan = parse(q)
    plan = opt(plan.to_plan())
    plan = Yield_Col(plan)
    plan.init_schema()
    q_rt = timeit.timeit(lambda: list(plan), number=20)
    data.append(dict(query=q, y=q_rt))
  return data

if __name__ == "__main__":
  # we need to pass in the database so that the database isn't recreated
  # on each query
  db = Database.db()
  data = performance_record(queries, db)
  print(data)


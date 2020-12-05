import time
import timeit
from databass import *
from databass.ops import *
from databass.exprutil import *

queries = [
  """SELECT O_CUSTKEY, sum(O_TOTALPRICE) FROM ORDERS WHERE O_ORDERSTATUS = 'O' GROUP BY O_CUSTKEY """,
]

def performance_record(qs, db):
  opt = Optimizer()
  data = []
  for q in qs:
    plan = parse(q)
    plan = opt(plan.to_plan())
    q_rt = timeit.timeit(lambda: plan.hand_in_result(), number=20)
    data.append(dict(query=q, y=q_rt))
  return data

if __name__ == "__main__":
  # we need to pass in the database so that the database isn't recreated
  # on each query
  db = Database.db()
  data = performance_record(queries, db)
  print(data)


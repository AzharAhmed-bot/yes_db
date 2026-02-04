# Test insert performance
import time
from chidb import YesDB

db = YesDB('perf_test.cdb')
db.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)')

start = time.time()
for i in range(80000):
    db.execute(f'INSERT INTO test VALUES (NULL, {i})')
end = time.time()

print(f"Inserted 80000 rows in {end-start:.2f} seconds")
print(f"Rate: {80000/(end-start):.0f} inserts/sec")

# Test select performance
start = time.time()
results = db.execute('SELECT * FROM test')
end = time.time()

print(f"Selected {len(results)} rows in {end-start:.2f} seconds")
db.close()
from redis import Redis
from rq import Queue
from tasks import slow_function

# Koneksi ke Redis
redis_conn = Redis()
q = Queue(connection=redis_conn)

# Mengirim 2 tugas ke antrean secara asinkron
job1 = q.enqueue(slow_function, 5)
job2 = q.enqueue(slow_function, 2)
job3 = q.enqueue(slow_function, 4)
print(f"Task dikirim, job ID: {job1.id}")
print(f"Task dikirim, job ID: {job2.id}")
print(f"Task dikirim, job ID: {job3.id}")
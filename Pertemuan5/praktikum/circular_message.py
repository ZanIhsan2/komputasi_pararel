# circular_message.py
from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

message = f"Halo dari proses {rank}"

# Kirim pesan ke proses berikutnya (secara melingkar)
dest = (rank + 1) % size
comm.send(message, dest=dest)

# Terima pesan dari proses sebelumnya
source = (rank - 1) % size
received = comm.recv(source=source)

# Simpan hasil
result = f"Proses {rank} menerima pesan: '{received}' dari proses {source}"

# Tunggu semua proses selesai sebelum mencetak
comm.Barrier()
time.sleep(rank * 0.1)  # jeda kecil supaya urutan tampil rapi
print(result)
from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
fh = MPI.File.Open(comm, "output.bin", MPI.MODE_RDONLY)
size = 18
offset = rank * size
data = bytearray(size)
fh.Read_at(offset, data)
print(f"Rank {rank} read: ", data.decode().strip())
fh.Close()
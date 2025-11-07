from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
fh = MPI.File.Open(comm, "output.bin", MPI.MODE_WRONLY | MPI.MODE_CREATE)
data = bytearray(f"Hello from rank {rank}\n", 'utf-8')
offset = rank * len(data)
fh.Write_at(offset, data)
fh.Close()
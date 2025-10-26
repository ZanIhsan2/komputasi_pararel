from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
if rank == 0:
    data = "Hallo dari proses 0"
    comm.send(data, dest=1)
    print(f"[{rank}] Mengirim data: {data}")
elif rank == 1:
    data = comm.recv(source=0)
    print(f"[{rank}] Menerima data: {data}")
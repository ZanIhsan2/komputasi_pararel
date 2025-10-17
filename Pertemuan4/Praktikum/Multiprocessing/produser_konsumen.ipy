import multiprocessing
import time

def produser(queue):
    for i in range(5):
        data = f"TaskID-{i+1}"
        queue.put(data)
        time.sleep(0.2) # Simulasikan I/O
        print(f"[Produser] Menghasilkan {data}")
    queue.put(None) # Sentinel (Penanda berhenti)

def konsumen(queue, id_worker):
    while True:
        data = queue.get()
        if data is None:
            queue.put(None) # Meneruskan sentinel ke worker lain
            break
        print(f"[Konsumen-{id_worker}] Memproses {data}")
        time.sleep(0.5) # Simulasikan beban CPU

if __name__ == '__main__':
    q = multiprocessing.Queue()
    p_produser = multiprocessing.Process(target=produser, args=(q,))
    # 2 Konsumen untuk menunjukkan pembagian tugas
    p_konsumen = [multiprocessing.Process(target=konsumen, args=(q, i+1)) for i in range(2)]
    
    p_produser.start()
    for p in p_konsumen:
        p.start()
        
    p_produser.join()
    for p in p_konsumen:
        p.join()
    # Output menunjukkan proses Produser dan Konsumen berjalan secara bersamaan
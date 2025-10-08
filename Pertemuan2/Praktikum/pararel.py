import multiprocessing
import time

def hitung_kuadrat(n):
    # Simulasi tugas yang memakan waktu
    time.sleep(0.1) 
    return n * n

if __name__ == "__main__":
    data = range(10)

    # Metode Serial
    start_time = time.time()
    hasil_serial = [hitung_kuadrat(n) for n in data]
    end_time = time.time()
    print(f"Waktu serial: {end_time - start_time:.2f} detik")

    # Metode Paralel dengan Pool
    start_time = time.time()
    with multiprocessing.Pool(processes=4) as pool:
        hasil_paralel = pool.map(hitung_kuadrat, data)
    end_time = time.time()
    print(f"Waktu paralel: {end_time - start_time:.2f} detik")
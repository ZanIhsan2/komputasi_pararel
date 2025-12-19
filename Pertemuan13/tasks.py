import time
def slow_function(n):
    print(f"Memulai tugas untuk {n} ....")
    time.sleep(n) # Simulasi proses berat
    print(f"Tugas {n} selesai!")
    return n * 10
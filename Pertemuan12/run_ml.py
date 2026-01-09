import time
from dask_ml.model_selection import train_test_split
from dask_ml.linear_model import LogisticRegression
from dask.distributed import Client, LocalCluster

import dask.array as da

# 1. Inisialisasi Klien Dask
# Asumsi Anda sudah menjalankan LocalCluster atau Dask Scheduler.
try:
    client = Client("tcp://localhost:8786")
    print("✅ Berhasil tersambung ke Dask Cluster!")
    print(f"Info Cluster: {client}")
except OSError:
    print("⚠️ Gagal tersambung ke scheduler di tcp://localhost:8786. Pastikan Dask Scheduler berjalan.")
    # Opsional: Buat LocalCluster jika koneksi gagal (hanya untuk pengujian)
    # cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    # client = Client(cluster)
    # print("✅ Membuat dan tersambung ke LocalCluster baru.")
    # print(f"Info Cluster: {client}")
    # return # Keluar jika tidak ada klien yang tersambung

# --- PENGUKURAN WAKTU UNTUK PRE-PEMROSESAN DATA ---
start_time_data = time.time()

# 2. Pembuatan Data Dummy Skala Besar
# Perlu dicatat: Dask adalah malas (lazy). Pembuatan array ini cepat.
# Waktu sebenarnya akan dihabiskan saat komputasi dimulai (e.g., di model.fit).
X = da.random.random((1000000, 20), chunks=(10000, 20)) # Meningkatkan ukuran data untuk demonstrasi
y = da.random.randint(0, 2, size=(1000000,), chunks=(10000,))
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

end_time_data = time.time()
data_time = end_time_data - start_time_data

print(f"\n⏰ Waktu untuk Pembuatan & Pembagian Data (Lazy): {data_time:.4f} detik")
print(f"Ukuran Data: {X.shape[0]} baris, {X.shape[1]} fitur")
print("-" * 50)

# --- PENGUKURAN WAKTU UNTUK PELATIHAN MODEL ---
model = LogisticRegression()

start_time_fit = time.time()

# 3. Pelatihan Model
# model.fit() adalah tempat Dask benar-benar menjalankan komputasi.
print("⏳ Memulai pelatihan model...")
model.fit(X_train, y_train)

end_time_fit = time.time()
fit_time = end_time_fit - start_time_fit

print("✅ Pelatihan model selesai!")
print(f"⏰ Waktu Eksekusi Pelatihan Model (model.fit()): {fit_time:.4f} detik")
print("-" * 50)

# 4. Total Waktu (opsional)
total_time = data_time + fit_time
print(f"⏱️ Total Waktu (Pre-proses Lazy + Pelatihan): {total_time:.4f} detik")

# 5. Opsional: Tutup Klien Dask
client.close()

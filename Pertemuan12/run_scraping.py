import requests
from dask.distributed import Client, LocalCluster
import dask.bag as db
import time
import os

# --- 1. Setup Dask Cluster ---

def setup_dask_client():
    """Menginisialisasi Dask Cluster (LocalCluster) atau terhubung ke yang sudah ada."""
    try:
        # Coba terhubung ke scheduler yang sudah berjalan
        client = Client("tcp://localhost:8786")
        print("✅ Berhasil tersambung ke Dask Cluster yang sudah ada.")
    except OSError:
        # Jika gagal, buat LocalCluster baru dengan 4 worker
        cluster = LocalCluster(n_workers=4, threads_per_worker=1, processes=True)
        client = Client(cluster)
        print(f"✅ Membuat dan tersambung ke LocalCluster baru dengan {len(cluster.workers)} worker.")
    
    print(f"Dashboard Dask Anda: {client.dashboard_link}\n")
    return client

# --- 2. Fungsi untuk Scraping ---

def fetch_url_content(url):
    """
    Fungsi scraping yang akan dijalankan oleh worker Dask secara paralel.
    Menyertakan penundaan 1 detik untuk mensimulasikan tugas yang substansial.
    """
    try:
        # Penundaan buatan: 1 detik per permintaan
        time.sleep(1) 
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return {
                'url': url,
                'status': 'SUCCESS',
                'content_length': len(response.text),
                'worker_pid': os.getpid() # Menunjukkan worker yang menjalankan tugas
            }
        else:
            return {
                'url': url,
                'status': 'FAILED',
                'content_length': 0,
                'error': f'Status Code: {response.status_code}',
                'worker_pid': os.getpid()
            }
    except requests.exceptions.RequestException as e:
        # Menangani error jaringan
        return {
            'url': url,
            'status': 'ERROR',
            'content_length': 0,
            'error': str(e),
            'worker_pid': os.getpid()
        }

# --- 3. Eksekusi Utama ---

def run_distributed_scraping():
    client = setup_dask_client()
    
    # Daftar inti URL (20 URL)
    core_urls = [
        "https://www.google.com", "https://www.bing.com", "https://docs.dask.org",
        "https://www.python.org", "https://pypi.org", "http://this-url-will-fail-on-purpose-123.com",
        "https://www.wikipedia.org", "https://github.com", "https://www.youtube.com",
        "https://www.reddit.com", "https://stackoverflow.com", "https://medium.com",
        "https://www.w3schools.com", "https://aws.amazon.com", "https://azure.microsoft.com",
        "https://cloud.google.com", "https://openai.com", "https://dask.org",
        "https://numpy.org", "https://pandas.pydata.org",
    ]
    
    # Perbanyak daftar URL menjadi 200 URL (20 * 10)
    urls_to_scrape = core_urls * 10

    total_urls = len(urls_to_scrape)
    num_workers = len(client.scheduler_info()['workers'])
    
    print(f"Memulai scraping untuk {total_urls} URL...")
    print(f"Jumlah Worker di Cluster: {num_workers}")
    print(f"Target Waktu Serial Minimum (Estimasi): {total_urls * 1:.2f} detik")
    print(f"Target Waktu Paralel Maksimum (Estimasi): {(total_urls / num_workers) * 1:.2f} detik (dengan {num_workers} worker)")
    print("-" * 50)
    
    start_time = time.time()
    
    # --- Perbaikan: Mendapatkan nilai integer dari client.nthreads ---
    # client.nthreads() harus dipanggil sebagai fungsi untuk mendapatkan jumlah threads.
    num_threads_per_worker = client.nthreads() 
    # 1. Buat Dask Bag dari daftar URL
    # Jumlah partisi diatur menjadi 2x jumlah thread per worker untuk pemanfaatan sumber daya yang optimal
    dask_bag = db.from_sequence(urls_to_scrape, npartitions=sum(num_threads_per_worker.values()) * 2)

    # 2. Map: Terapkan fungsi 'fetch_url_content'
    print(f"Membagi tugas menjadi {dask_bag.npartitions} partisi...")
    results_bag = dask_bag.map(fetch_url_content)

    # 3. Compute: Memicu eksekusi dan kumpulkan hasilnya
    print("⏳ Menunggu Dask Cluster menyelesaikan semua tugas scraping...")
    
    # .compute() memicu eksekusi terdistribusi
    final_results = results_bag.compute()
    
    end_time = time.time()
    
    # --- 4. Ringkasan Hasil ---

    successes = sum(1 for r in final_results if r['status'] == 'SUCCESS')
    failures = len(final_results) - successes
    
    processing_time = end_time - start_time
    
    print("\n" + "=" * 50)
    print(f"✅ Scraping Selesai!")
    print(f"⏰ Total Waktu Eksekusi (Paralel): {processing_time:.2f} detik")
    print(f"Total URL Diproses: {total_urls}")
    print(f"Berhasil (SUCCESS): {successes}")
    print(f"Gagal/Error: {failures}")
    print("=" * 50)
    
    # Contoh menampilkan 5 hasil pertama
    print("\n--- 5 Hasil Pertama ---")
    for result in final_results[:5]:
        print(result)

    client.close()
    
# Jalankan skrip
if __name__ == "__main__":
    run_distributed_scraping()

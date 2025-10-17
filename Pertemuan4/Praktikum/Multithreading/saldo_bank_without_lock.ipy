import threading
import time

saldo = 1000

def transaksi(jumlah, tipe):
    global saldo
    new_saldo = saldo
    try:
        current_saldo = saldo
        time.sleep(0.0001) # Simulasikan latensi
        
        if tipe == 'debit':
            new_saldo = current_saldo - jumlah
        elif tipe == 'kredit':
            new_saldo = current_saldo + jumlah
        
        saldo = new_saldo # Tulis hasil akhir
    finally:
        pass
        
if __name__ == '__main__':
    print(f"Saldo Awal: Rp {saldo}")

    # ... Inisialisasi thread dan eksekusi ...
    # Saldo Akhir yang Benar (5 debit @50, 3 kredit @30) = 840
    # Demonstrasikan: Jalankan TANPA Lock untuk melihat hasil ACAK.

    a = threading.Thread(target=transaksi, args=(50,'debit',))
    b = threading.Thread(target=transaksi, args=(50,'debit',))
    c = threading.Thread(target=transaksi, args=(30,'kredit',))
    d = threading.Thread(target=transaksi, args=(50,'debit',))
    e = threading.Thread(target=transaksi, args=(30,'kredit',))
    f = threading.Thread(target=transaksi, args=(50,'debit',))
    g = threading.Thread(target=transaksi, args=(50,'debit',))
    h = threading.Thread(target=transaksi, args=(30,'kredit',))
    threads = []
    for tr in [a,b,c,d,e,f,g,h]:
        tr.start()
        threads.append(tr)
    for t in threads: 
        t.join()
    print(f"Saldo Akhir: Rp {saldo}")
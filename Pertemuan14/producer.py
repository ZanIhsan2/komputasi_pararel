from tasks import square
result = square.delay(10)
print("Task dikirim")

# Mengambil hasil
print(result.get())
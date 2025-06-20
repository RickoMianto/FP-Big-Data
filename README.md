# FP-Big-Data

## Anggota Kelompok 8

| No  | Nama Lengkap      | NRP        |
| --- | ----------------- | ---------- |
| 1   | Irfan Qobus Salim | 5027221058 |
| 2   | Ricko Mianto J S  | 5027231031 |
| 3   | Raditya Hardian S | 5027231033 |
| 4   | Gallant Damas H   | 5027231037 |

## Struktur Directory

```
e-commerce-recommender/
├── docker-compose.yml          # File untuk mengatur dan menjalankan semua service dengan Docker
|
├── run_project.py              # otomasi untuk running keseluruhan project
|
├── data/
│   ├── ecommerce-events.csv      # Dataset mentah
│   └── images/                   # Dataset gambar yang di crawling
|       ├── accessories_bag/
|       ├── accessories_umbrella/
|       └── accessories_wallet/
|
├── kafka_producer/
│   ├── requirements.txt        # Kebutuhan library (kafka-python)
│   └── producer.py             # Script untuk membaca CSV dan mengirim ke Kafka
|
├── spark_processor/
│   ├── jobs/
│   │   ├── 1_bronze_to_silver.py   # Job Spark: Membaca dari Kafka/Bronze, membersihkan, simpan ke Silver
│   │   ├── 2_silver_to_gold.py     # Job Spark: Agregasi statistik, simpan ke Gold
│   │   └── 3_train_model.py        # Job Spark: Melatih model ML, simpan hasil/model ke Gold
│   └── requirements.txt        # Kebutuhan library (pyspark)
|
├── api_server/
│   ├── app.py                  # Logic utama Flask API
│   ├── minio_client.py         # Helper untuk koneksi ke MinIO
│   └── requirements.txt        # Kebutuhan library (flask, boto3/minio)
|
├── frontend/
|   ├── index.html              # Halaman utama website
|   ├── css/
|   │   └── style.css           # Styling
|   └── js/
|       └── app.js              # Logic Javascript untuk fetch data dari API dan menampilkannya
|
└── README.md                   # Penjelasan proyek, cara setup, dan cara menjalankan
```

## Flowchart 

![WhatsApp Image 2025-06-20 at 14 08 42_9969aa66](https://github.com/user-attachments/assets/b33ae50a-6c20-4cd0-9b51-80b809872088)

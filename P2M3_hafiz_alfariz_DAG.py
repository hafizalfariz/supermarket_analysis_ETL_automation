'''
=================================================
Milestone 3 - ETL Pipeline Supermarket Sales

Nama   : Hafiz Alfariz
Batch  : FTDS-045-RMT

Deskripsi:
Pipeline ini mengotomasi proses ETL data penjualan supermarket menggunakan Apache Airflow. 
Data diambil dari PostgreSQL, dibersihkan dan divalidasi, lalu diupload ke Elasticsearch untuk analisis dan visualisasi di Kibana.

Tahapan utama:
1. Fetch Data: Mengambil data mentah dari PostgreSQL (table_m3) dan menyimpannya sebagai CSV.
2. Data Cleaning: Membersihkan data (hapus duplikat, normalisasi kolom, konversi tipe data, handling missing values) dan menyimpan hasilnya ke CSV.
3. Upload ke Elasticsearch: Mengirim data bersih ke Elasticsearch dengan id unik untuk setiap transaksi.

Fitur tambahan:
- Penjadwalan otomatis setiap Sabtu jam 09:10, 09:20, dan 09:30 WIB.
- Mendukung proses EDA dan dashboard di Kibana.

Tujuan:
Memastikan data yang digunakan untuk analisis bisnis selalu bersih, valid, dan up-to-date secara otomatis.
=================================================
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch

# Fungsi untuk fetch data dari PostgreSQL
def fetch_from_postgresql(**kwargs):
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow',
        user='airflow',
        password='airflow'
    )
    df = pd.read_sql('SELECT * FROM table_m3', conn)
    conn.close()
    # Simpan ke file 
    df.to_csv('/opt/airflow/dags/P2M3_hafiz_alfariz_data_raw.csv', index=False)
    # Push ke XCom agar bisa diambil task berikutnya
    kwargs['ti'].xcom_push(key='raw_data_path', value='/opt/airflow/dags/P2M3_hafiz_alfariz_data_raw.csv')

# Fungsi untuk data cleaning
def data_cleaning(**kwargs):
    import os
    ti = kwargs['ti']
    raw_data_path = ti.xcom_pull(key='raw_data_path', task_ids='fetch_from_postgresql')
    df = pd.read_csv(raw_data_path)
    # Hapus duplikat
    df = df.drop_duplicates()
    # Normalisasi nama kolom
    df.columns = [col.strip().lower().replace(' ', '_').replace('%','pct') for col in df.columns]
    # Konversi tipe data
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], format='%H:%M', errors='coerce').dt.time
    num_cols = ['unit_price','quantity','tax_5pct','sales','cogs','gross_margin_percentage','gross_income','rating']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.fillna(df.median(numeric_only=True))
    df = df.fillna(df.mode().iloc[0])
    clean_path = '/opt/airflow/dags/P2M3_hafiz_alfariz_data_clean.csv'
    df.to_csv(clean_path, index=False)
    ti.xcom_push(key='clean_data_path', value=clean_path)

# Fungsi untuk post ke Elasticsearch
def post_to_elasticsearch(**kwargs):
    '''
    Fungsi: post_to_elasticsearch

    Deskripsi:
    Mengirim data hasil cleaning ke Elasticsearch agar dapat digunakan untuk analisis dan visualisasi di Kibana. 
    Fungsi ini membaca file CSV hasil pembersihan data, mengubah setiap baris menjadi dokumen, dan mengupload ke index 'supermarket_sales' di Elasticsearch.

    Langkah-langkah:
    1. Mengambil path file CSV hasil cleaning dari XCom (output task sebelumnya).
    2. Membaca data CSV ke dalam DataFrame pandas.
    3. Melakukan iterasi pada setiap baris data:
    - Mengubah baris menjadi dictionary.
    - Menangani missing value dan tipe data agar sesuai format Elasticsearch.
    - Menggunakan 'invoice_id' sebagai id unik dokumen.
    4. Mengupload setiap dokumen ke Elasticsearch pada index 'supermarket_sales'.

    Catatan:
    - Koneksi menggunakan host 'elasticsearch' agar terhubung antar container di Docker Compose.
    - Pastikan Elasticsearch sudah berjalan dan index 'supermarket_sales' sudah tersedia.
    - Data yang diupload sudah bersih dan tervalidasi dari proses cleaning sebelumnya.

    Tujuan:
    Memastikan data bersih dan siap dianalisis di platform NoSQL (Elasticsearch) untuk mendukung proses EDA dan pembuatan dashboard di Kibana.
    '''
    ti = kwargs['ti']
    clean_data_path = ti.xcom_pull(key='clean_data_path', task_ids='data_cleaning')
    es = Elasticsearch(['http://elasticsearch:9200'])
    df = pd.read_csv(clean_data_path)
    for _, row in df.iterrows():
        doc = row.to_dict()
        for k, v in doc.items():
            if pd.isnull(v):
                doc[k] = None
            elif isinstance(v, (pd.Timestamp, pd.Timedelta)):
                doc[k] = str(v)
        es.index(index='supermarket_sales', id=doc.get('invoice_id'), body=doc)

default_args = {
    'owner': 'hafiz_alfariz',
    'start_date': datetime(2024, 11, 1, 9, 10) - timedelta(hours=7),  # WIB (UTC+7)
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'P2M3_hafiz_alfariz_DAG',
    default_args=default_args,
    schedule_interval='10,20,30 9 * * 6',  # Setiap Sabtu jam 09:10, 09:20, 09:30
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_from_postgresql',
    python_callable=fetch_from_postgresql,
    provide_context=True,
    dag=dag
)

clean_task = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    provide_context=True,
    dag=dag
)

post_task = PythonOperator(
    task_id='post_to_elasticsearch',
    python_callable=post_to_elasticsearch,
    provide_context=True,
    dag=dag
)

fetch_task >> clean_task >> post_task
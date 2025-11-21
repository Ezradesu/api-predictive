from flask import Flask, jsonify, request
from gradio_client import Client as GradioClient
from supabase import create_client, Client
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# --- KONFIGURASI ---
# Pastikan ENV VARIABLES sudah di-set di Vercel Dashboard
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SPACE_ID = "Ezradesu/predictive-maintenance-gradio"
SENSOR_TABLE = "datasets"
TICKET_TABLE = "failure_ticket"

# Inisialisasi Client
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    gradio_client = GradioClient(SPACE_ID)
except Exception as e:
    print(f"Init Error: {e}")

@app.route('/api/process', methods=['GET'])
def process_data():
    print("⏳ Memulai pengecekan data...")

    try:
        # 1. Ambil 5 Data Sensor Terbaru
        response = supabase.table(SENSOR_TABLE).select("*").limit(5).order("udi", desc=True).execute()
        all_sensor_data = response.data

        if not all_sensor_data:
            return jsonify({"status": "No Data Source", "count": 0})

        # --- LOGIKA BARU: ANTI DUPLIKAT ---

        # A. Kumpulkan semua UDI dari data yang diambil
        udis_to_check = [row['udi'] for row in all_sensor_data]

        # B. Cek ke tabel failure_ticket: ID mana yang SUDAH ada?
        # Kita query tabel tiket dimana 'sensor_id' ada di dalam list 'udis_to_check'
        existing_tickets = supabase.table(TICKET_TABLE).select("sensor_id").in_("sensor_id", udis_to_check).execute()

        # Buat list ID yang sudah diproses (supaya pencarian cepat)
        processed_ids = [item['sensor_id'] for item in existing_tickets.data]

        # C. Filter: Hanya ambil data yang ID-nya BELUM ada di processed_ids
        new_sensor_data = [row for row in all_sensor_data if row['udi'] not in processed_ids]

        if not new_sensor_data:
            print("✅ Semua data terbaru sudah diproses sebelumnya. Tidak ada tindakan.")
            return jsonify({"status": "Skipped (All Duplicate)", "processed": 0})

        print(f"found {len(new_sensor_data)} data baru yang belum diproses.")

        # ----------------------------------

        results_to_insert = []

        # 2. Loop Processing (Hanya data baru)
        for row in new_sensor_data:
            try:
                # Mapping & Gradio Call
                type_str_raw = row['type']
                mapping = {"l": "L (Low Quality)", "m": "M (Medium Quality)", "h": "H (High Quality)"}
                type_str_gradio = mapping.get(type_str_raw.lower())

                if not type_str_gradio: continue

                # Panggil AI
                output_data = gradio_client.predict(
                    type_input=type_str_gradio,
                    air_temp=float(row['air_temperature_k']),
                    process_temp=float(row['process_temperature_k']),
                    rpm=int(row['rotational_speed_rpm']),
                    torque=float(row['torque_nm']),
                    tool_wear=int(row['tool_wear_min']),
                    api_name="/predict"
                )

                status, severity, confidence_str, saran = output_data
                confidence = float(confidence_str.replace('%', '')) / 100.0 

                ticket = {
                    "sensor_id": row['udi'], 
                    "failure_status": status,
                    "severity_level": severity,
                    "confidence_score": confidence,
                    "recommendation": saran,
                    "is_active": True
                }
                results_to_insert.append(ticket)

            except Exception as e:
                print(f"Error processing row {row.get('udi')}: {e}")
                continue

        # 3. Simpan ke DB
        if results_to_insert:
            supabase.table(TICKET_TABLE).insert(results_to_insert).execute()
            count = len(results_to_insert)
            return jsonify({"status": "Success", "processed": count, "ids": [r['sensor_id'] for r in results_to_insert]})

        return jsonify({"status": "No valid prediction generated", "processed": 0})

    except Exception as e:
        return jsonify({"status": "System Error", "message": str(e)}), 500



@app.route('/')
def home():
    return "Smart Maintenance Pipeline Ready."

if __name__ == "__main__":
    print("🚀 Starting Flask server...")
    app.run(host="0.0.0.0", port=5000, debug=True)

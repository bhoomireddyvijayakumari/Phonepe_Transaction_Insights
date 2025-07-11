import os
import json
import mysql.connector

# ---------- DB CONNECTION ----------
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Password',  # 🔁 Replace with your actual MySQL password
    database='ppp'
)
cursor = connection.cursor()

# ---------- BASE PATH ----------
base_dir = os.path.expanduser("~/Desktop/pulse/data")

# ---------- Helper ----------
def get_year_quarter(filename):
    try:
        return int(filename.strip('.json').split('-')[0]), int(filename.strip('.json').split('-')[-1])
    except:
        return None, None

# ---------- INSERT FUNCTION 1: Aggregated User ----------
def insert_aggregated_user():
    print("\n🔄 Inserting into aggregated_user...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'aggregated', 'user', 'country', 'india', 'state')
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                users = data.get('data', {}).get('usersByDevice')
                if not users:
                    print(f"⚠️ Skipped: {file_path} (no usersByDevice)")
                    continue
                _, quarter = get_year_quarter(file)
                for entry in users:
                    try:
                        cursor.execute("""
                            INSERT INTO aggregated_user (state, year, quarter, brand, count, percentage)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            state,
                            int(year),
                            quarter,
                            entry['brand'],
                            entry['count'],
                            entry['percentage']
                        ))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM aggregated_user")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")
# ---------- INSERT FUNCTION 2: Aggregated Transaction ----------
def insert_aggregated_transaction():
    print("\n🔄 Inserting into aggregated_transaction...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'aggregated', 'transaction', 'country', 'india', 'state')
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                transactions = data.get('data', {}).get('transactionData')
                if not transactions:
                    print(f"⚠️ Skipped: {file_path} (no transactionData)")
                    continue
                _, quarter = get_year_quarter(file)
                for txn in transactions:
                    try:
                        txn_type = txn['name']
                        count = txn['paymentInstruments'][0]['count']
                        amount = txn['paymentInstruments'][0]['amount']
                        cursor.execute("""
                            INSERT INTO aggregated_transaction (state, year, quarter, txn_type, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, txn_type, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM aggregated_transaction")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- INSERT FUNCTION 3: Aggregated Insurance ----------
def insert_aggregated_insurance():
    print("\n🔄 Inserting into aggregated_insurance...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'aggregated', 'insurance', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                transactions = data.get('data', {}).get('transactionData')
                if not transactions:
                    print(f"⚠️ Skipped: {file_path} (no transactionData)")
                    continue
                _, quarter = get_year_quarter(file)
                for txn in transactions:
                    try:
                        ins_type = txn['name']
                        count = txn['paymentInstruments'][0]['count']
                        amount = txn['paymentInstruments'][0]['amount']
                        cursor.execute("""
                            INSERT INTO aggregated_insurance (state, year, quarter, insurance_type, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, ins_type, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM aggregated_insurance")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")
# ---------- INSERT FUNCTION 4: Map User ----------
def insert_map_user():
    print("\n🔄 Inserting into map_user...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'map', 'user', 'hover', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                hover_data = data.get('data', {}).get('hoverData')
                if not hover_data:
                    print(f"⚠️ Skipped: {file_path} (no hoverData)")
                    continue
                _, quarter = get_year_quarter(file)
                for district, info in hover_data.items():
                    try:
                        registered = info.get('registeredUsers', 0)
                        app_opens = info.get('appOpens', 0)
                        cursor.execute("""
                            INSERT INTO map_user (state, year, quarter, district, registered_users, app_opens)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, registered, app_opens))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM map_user")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- INSERT FUNCTION 5: Map Transaction ----------
def insert_map_map():
    print("\n🔄 Inserting into map_map...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'map', 'transaction', 'hover', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                hover_data = data.get('data', {}).get('hoverDataList')
                if not hover_data:
                    print(f"⚠️ Skipped: {file_path} (no hoverDataList)")
                    continue
                _, quarter = get_year_quarter(file)
                for item in hover_data:
                    try:
                        district = item['name']
                        count = item['metric'][0]['count']
                        amount = item['metric'][0]['amount']
                        cursor.execute("""
                            INSERT INTO map_map (state, year, quarter, district, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM map_map")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- INSERT FUNCTION 6: Map Insurance ----------
def insert_map_insurance():
    print("\n🔄 Inserting into map_insurance...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'map', 'insurance', 'hover', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                hover_data = data.get('data', {}).get('hoverDataList')
                if not hover_data:
                    print(f"⚠️ Skipped: {file_path} (no hoverDataList)")
                    continue
                _, quarter = get_year_quarter(file)
                for item in hover_data:
                    try:
                        district = item['name']
                        count = item['metric'][0]['count']
                        amount = item['metric'][0]['amount']
                        cursor.execute("""
                            INSERT INTO map_insurance (state, year, quarter, district, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM map_insurance")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")
# ---------- INSERT FUNCTION 7: Top User ----------
def insert_top_user():
    print("\n🔄 Inserting into top_user...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'top', 'user', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                users = data.get('data', {}).get('districts')
                if not users:
                    print(f"⚠️ Skipped: {file_path} (no districts)")
                    continue
                _, quarter = get_year_quarter(file)
                for entry in users:
                    try:
                        district = entry['name']
                        registered = entry['registeredUsers']
                        cursor.execute("""
                            INSERT INTO top_user (state, year, quarter, district, registered_users)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, registered))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM top_user")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- INSERT FUNCTION 8: Top Transaction ----------
def insert_top_map():
    print("\n🔄 Inserting into top_map...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'top', 'transaction', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                entries = data.get('data', {}).get('districts')
                if not entries:
                    print(f"⚠️ Skipped: {file_path} (no districts)")
                    continue
                _, quarter = get_year_quarter(file)
                for entry in entries:
                    try:
                        district = entry['entityName']
                        count = entry['metric']['count']
                        amount = entry['metric']['amount']
                        cursor.execute("""
                            INSERT INTO top_map (state, year, quarter, district, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM top_map")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- INSERT FUNCTION 9: Top Insurance ----------
def insert_top_insurance():
    print("\n🔄 Inserting into top_insurance...")
    total_inserted = 0
    folder = os.path.join(base_dir, 'top', 'insurance', 'country', 'india', 'state')
    if not os.path.exists(folder): return
    for state in os.listdir(folder):
        state_path = os.path.join(folder, state)
        if not os.path.isdir(state_path): continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path): continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"): continue
                file_path = os.path.join(year_path, file)
                with open(file_path) as f:
                    data = json.load(f)
                entries = data.get('data', {}).get('districts')
                if not entries:
                    print(f"⚠️ Skipped: {file_path} (no districts)")
                    continue
                _, quarter = get_year_quarter(file)
                for entry in entries:
                    try:
                        district = entry['entityName']
                        count = entry['metric']['count']
                        amount = entry['metric']['amount']
                        cursor.execute("""
                            INSERT INTO top_insurance (state, year, quarter, district, count, amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, int(year), quarter, district, count, amount))
                        total_inserted += 1
                    except Exception as e:
                        print(f"❌ Error inserting from {file_path}: {e}")
    connection.commit()
    cursor.execute("SELECT COUNT(*) FROM top_insurance")
    print(f"✅ Inserted {total_inserted}, Total: {cursor.fetchone()[0]}")

# ---------- MAIN RUN BLOCK ----------
if __name__ == '__main__':
    insert_aggregated_user()
    insert_aggregated_transaction()
    insert_aggregated_insurance()
    insert_map_user()
    insert_map_map()
    insert_map_insurance()
    insert_top_user()
    insert_top_map()
    insert_top_insurance()

    cursor.close()
    connection.close()
    print("\n✅ All JSON data inserted into MySQL successfully.")

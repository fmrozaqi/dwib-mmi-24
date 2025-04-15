import duckdb
import pandas as pd
from datetime import datetime

class ETLPipeline:
    """Class untuk mengotomatisasi proses ETL"""
    
    def __init__(self, db_path='config/brazil_stock_market.db'):
        """Inisialisasi pipeline"""
        self.conn = duckdb.connect(db_path)
        self.initialized = False
        self.log_table_setup()
    
    def log_table_setup(self):
        """Membuat tabel log untuk melacak proses ETL"""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS "etlLog" (
            "logId" INTEGER PRIMARY KEY,
            "processName" VARCHAR,
            "startTime" TIMESTAMP,
            "endTime" TIMESTAMP,
            "recordsProcessed" INTEGER,
            "status" VARCHAR,
            "message" VARCHAR
        )
        """)
    
    def log_process_start(self, process_name):
        """Mencatat mulainya proses ETL"""
        log_id = self.conn.execute('SELECT COALESCE(MAX(logId), 0) + 1 FROM "etlLog"').fetchone()[0]
        self.conn.execute("""
        INSERT INTO "etlLog" (logId, processName, startTime, status)
        VALUES (?, ?, CURRENT_TIMESTAMP, 'RUNNING')
        """, [log_id, process_name])
        return log_id
    
    def log_process_end(self, log_id, records=0, status='SUCCESS', message=None):
        """Mencatat selesainya proses ETL"""
        self.conn.execute("""
        UPDATE "etlLog"
        SET endTime = CURRENT_TIMESTAMP,
            recordsProcessed = ?,
            status = ?,
            message = ?
        WHERE logId = ?
        """, [records, status, message, log_id])
                
    def reset_tables(self):
        self.conn.execute(f"DROP TABLE IF EXISTS factCoins")
        self.conn.execute(f"DROP TABLE IF EXISTS factStocks")

        # Get list of all tables in the 'main' schema
        tables = self.conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()

        # Drop each table
        for table in tables:
            table_name = table[0]
            if not table_name == "etlLog":
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        print("All tables reset.")
                
    def initialize_warehouse(self):
        """Inisialisasi skema data warehouse jika belum ada"""
        if self.initialized:
            return
        
        self.reset_tables()
        log_id = self.log_process_start("initialize_warehouse")
        
        try:
            # Dimensi Company
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS "dimCompany" (
                "keyCompany" INT NOT NULL,
                "stockCodeCompany" VARCHAR(32) NOT NULL,
                "nameCompany" VARCHAR(64) NOT NULL,
                "sectorCodeCompany" VARCHAR(32) NOT NULL,
                "sectorCompany" VARCHAR(256) NOT NULL,
                "segmentCompany" VARCHAR(256) NOT NULL,
                "startedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
                "endedAt" TIMESTAMP NULL,
                "isActive" BOOLEAN NOT NULL DEFAULT TRUE,
                
                CONSTRAINT companyPK PRIMARY KEY ("keyCompany")
            );
            """)
            
            # Dimensi Coin
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS "dimCoin" (
                "keyCoin" INT NOT NULL,
                "abbrevCoin" VARCHAR(32) NOT NULL,
                "nameCoin" VARCHAR(32) NOT NULL,
                "symbolCoin" VARCHAR(8) NOT NULL,
                "startedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
                "endedAt" TIMESTAMP NULL,
                "isActive" BOOLEAN NOT NULL DEFAULT TRUE,
                
                CONSTRAINT coinPK PRIMARY KEY (keyCoin)
            );
            """)
            
            # Dimensi Time
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS "dimTime" (
                "keyTime" INT NOT NULL,
                "datetime" VARCHAR(32) NOT NULL,
                "dayTime" SMALLINT NOT NULL,
                "dayWeekTime" SMALLINT NOT NULL,
                "dayWeekAbbrevTime" VARCHAR(32) NOT NULL,
                "dayWeekCompleteTime" VARCHAR(32) NOT NULL,
                "monthTime" SMALLINT NOT NULL,
                "monthAbbrevTime" VARCHAR(32) NOT NULL,
                "monthCompleteTime" VARCHAR(32) NOT NULL,
                "bimonthTime" SMALLINT NOT NULL,
                "quarterTime" SMALLINT NOT NULL,
                "semesterTime" SMALLINT NOT NULL,
                "yearTime" INT NOT NULL,
                
                CONSTRAINT timePK PRIMARY KEY ("keyTime")
            );
            """)
            
            # Tabel fakta Coins
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS "factCoins" (
                "keyTime" INT NOT NULL,
                "keyCoin" INT NOT NULL,
                "valueCoin" FLOAT NOT NULL,
                
                FOREIGN KEY (keyTime) REFERENCES dimTime(keyTime),
                FOREIGN KEY (keyCoin) REFERENCES dimCoin(keyCoin),
                CONSTRAINT coinsPK PRIMARY KEY(keyTime, keyCoin)
            );
            """)
            
            # Tabel fakta Stocks
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS "factStocks" (
                "keyTime" INT NOT NULL,
                "keyCompany" INT NOT NULL,
                "openValueStock" FLOAT NOT NULL,
                "closeValueStock" FLOAT NOT NULL,
                "highValueStock" FLOAT NOT NULL,
                "lowValueStock" FLOAT NOT NULL,
                "quantityStock" FLOAT NOT NULL,
                
                FOREIGN KEY (keyTime) REFERENCES dimTime(keyTime),
                FOREIGN KEY (keyCompany) REFERENCES dimCompany(keyCompany),
                CONSTRAINT stocksPK PRIMARY KEY(keyTime, keyCompany)
            );
            """)
            
            # Berhasil inisialisasi
            self.initialized = True
            self.log_process_end(log_id, status='SUCCESS', message='Data warehouse schema initialized')
            
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
    
    def extract_all_sources(self):
        """Ekstrak data dari semua sumber"""
        log_id = self.log_process_start("extract_all_sources")
        
        try:
            # Bersihkan tabel staging
            self.conn.execute("DROP TABLE IF EXISTS stagingCoin")
            self.conn.execute("DROP TABLE IF EXISTS stagingCompany")
            self.conn.execute("DROP TABLE IF EXISTS stagingTime")
            self.conn.execute("DROP TABLE IF EXISTS stagingCoinValue")
            self.conn.execute("DROP TABLE IF EXISTS stagingStockValue")
            
            # Ekstrak data dari file CSV
            # Create staging tables from CSVs
            self.conn.execute("CREATE TABLE stagingCoin AS SELECT * FROM read_csv_auto('config/data/dimCoin.csv')")
            self.conn.execute("CREATE TABLE stagingCompany AS SELECT * FROM read_csv_auto('config/data/dimCompany.csv')")
            self.conn.execute("CREATE TABLE stagingTime AS SELECT * FROM read_csv_auto('config/data/dimTime.csv')")
            self.conn.execute("CREATE TABLE stagingCoinValue AS SELECT * FROM read_csv_auto('config/data/factCoins.csv')")
            self.conn.execute("CREATE TABLE stagingStockValue AS SELECT * FROM read_csv_auto('config/data/factStocks.csv')")
            
            # Hitung total jumlah catatan
            total_records = 0
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingCoin").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingCompany").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingTime").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingCoinValue").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingStockValue").fetchone()[0]
            
            self.log_process_end(log_id, records=total_records, status='SUCCESS')
            return True
            
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
    
    def transform_data(self):
        """Transformasi data dari tabel staging"""
        log_id = self.log_process_start("transform_data")
        
        try:
            # 1. Transformasi dimensi Coin
            self.conn.execute("""
            CREATE OR REPLACE TABLE stagingDimCoin AS
            SELECT
                keyCoin,
                abbrevCoin,
                nameCoin,
                symbolCoin,
                NOW() startedAt,
                NULL endedAt,
                TRUE isActive
            FROM stagingCoin
            """)

            # 2. Transformasi dimensi Company
            self.conn.execute("""
            CREATE OR REPLACE TABLE stagingDimCompany AS
            SELECT
                keyCompany,
                stockCodeCompany,
                nameCompany,
                sectorCodeCompany,
                sectorCompany,
                segmentCompany,
                NOW() startedAt,
                NULL endedAt,
                TRUE isActive
            FROM stagingCompany
            """)

            # 3. Transformasi dimensi Time dengan translasi nama hari & bulan ke Bahasa Inggris
            self.conn.execute("""
            CREATE OR REPLACE TABLE stagingDimTime AS
            SELECT
                keyTime,
                datetime,
                dayTime,
                dayWeekTime,

                CASE dayWeekAbbrevTime
                    WHEN 'SEG' THEN 'MON'
                    WHEN 'TER' THEN 'TUE'
                    WHEN 'QUA' THEN 'WED'
                    WHEN 'QUI' THEN 'THU'
                    WHEN 'SEX' THEN 'FRI'
                    WHEN 'SAB' THEN 'SAT'
                    WHEN 'DOM' THEN 'SUN'
                    ELSE dayWeekAbbrevTime
                END dayWeekAbbrevTime,

                CASE dayWeekCompleteTime
                    WHEN 'SEGUNDA' THEN 'MONDAY'
                    WHEN 'TERCA' THEN 'TUESDAY'
                    WHEN 'QUARTA' THEN 'WEDNESDAY'
                    WHEN 'QUINTA' THEN 'THURSDAY'
                    WHEN 'SEXTA' THEN 'FRIDAY'
                    WHEN 'SABADO' THEN 'SATURDAY'
                    WHEN 'DOMINGO' THEN 'SUNDAY'
                    ELSE dayWeekCompleteTime
                END dayWeekCompleteTime,
                
                monthTime,

                CASE monthAbbrevTime
                    WHEN 'JAN' THEN 'JAN'
                    WHEN 'FEV' THEN 'FEB'
                    WHEN 'MAR' THEN 'MAR'
                    WHEN 'ABR' THEN 'APR'
                    WHEN 'MAI' THEN 'MAY'
                    WHEN 'JUN' THEN 'JUN'
                    WHEN 'JUL' THEN 'JUL'
                    WHEN 'AGO' THEN 'AUG'
                    WHEN 'SET' THEN 'SEP'
                    WHEN 'OUT' THEN 'OCT'
                    WHEN 'NOV' THEN 'NOV'
                    WHEN 'DEZ' THEN 'DEC'
                    ELSE monthAbbrevTime
                END monthAbbrevTime,
                
                CASE monthCompleteTime
                    WHEN 'JANEIRO' THEN 'JANUARY'
                    WHEN 'FEVEREIRO' THEN 'FEBRUARY'
                    WHEN 'MARCO' THEN 'MARCH'
                    WHEN 'ABRIL' THEN 'APRIL'
                    WHEN 'MAIO' THEN 'MAY'
                    WHEN 'JUNHO' THEN 'JUNE'
                    WHEN 'JULHO' THEN 'JULY'
                    WHEN 'AGOSTO' THEN 'AUGUST'
                    WHEN 'SETEMBRO' THEN 'SEPTEMBER'
                    WHEN 'OUTUBRO' THEN 'OCTOBER'
                    WHEN 'NOVEMBRO' THEN 'NOVEMBER'
                    WHEN 'DEZEMBRO' THEN 'DECEMBER'
                    ELSE monthCompleteTime
                END monthCompleteTime,

                bimonthTime,
                quarterTime,
                semesterTime,
                yearTime
            FROM stagingTime
            """)

            # 4. Transformasi fakta Coins
            self.conn.execute("""
            CREATE OR REPLACE TABLE stagingFactCoins AS
            SELECT
                keyTime,
                keyCoin,
                valueCoin
            FROM stagingCoinValue
            """)

            # 5. Transformasi fakta Stocks
            self.conn.execute("""
            CREATE OR REPLACE TABLE stagingFactStocks AS
            SELECT
                keyTime,
                keyCompany,
                openValueStock,
                closeValueStock,
                highValueStock,
                lowValueStock,
                quantityStock
            FROM stagingStockValue
            """)
            
            # Hitung jumlah catatan
            total_records = 0
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingDimCoin").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingDimCompany").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingDimTime").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingFactCoins").fetchone()[0]
            total_records += self.conn.execute("SELECT COUNT(*) FROM stagingFactStocks").fetchone()[0]

            self.log_process_end(log_id, records=total_records, status='SUCCESS')
            return True
            
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
    
    def delete_tables_data(self):
        # Hapus data tabel
        self.conn.execute("TRUNCATE factCoins")
        self.conn.execute("TRUNCATE factStocks")
        self.conn.execute("TRUNCATE dimCoin")
        self.conn.execute("TRUNCATE dimCompany")
        self.conn.execute("TRUNCATE dimTime")
    
    def load_data(self):
        """Load data ke data warehouse"""
        # Check if load_data has already been executed successfully
        check_query = """
        SELECT COUNT(*) FROM "etlLog" 
        WHERE "processName" = 'load_data' 
        AND "status" = 'SUCCESS'
        """
        already_executed = self.conn.execute(check_query).fetchone()[0] > 0
        
        if already_executed:
            print("load_data process already executed successfully. Skipping...")
            return True

        log_id = self.log_process_start("load_data")
        
        try:            
            # 1. Muat dimensi Coin
            self.conn.execute("INSERT INTO dimCoin SELECT * FROM stagingDimCoin")
            
            # 2. Muat dimensi Company
            self.conn.execute("INSERT INTO dimCompany SELECT * FROM stagingDimCompany")
            
            # 3. Muat dimensi Time
            self.conn.execute("INSERT INTO dimTime SELECT * FROM stagingDimTime")
            
            # 4. Muat fakta Coins dengan pemetaan kunci yang benar
            self.conn.execute("""
            INSERT INTO factCoins
            SELECT 
                t.keyTime,
                c.keyCoin,
                f.valueCoin
            FROM stagingFactCoins f
            JOIN dimTime t ON f.keyTime = t.keyTime
            JOIN dimCoin c ON f.keyCoin = c.keyCoin
            """)
            
            # 5. Muat fakta Stocks dengan pemetaan kunci yang benar
            self.conn.execute("""
            INSERT INTO factStocks
            SELECT 
                t.keyTime,
                c.keyCompany,
                f.openValueStock,
                f.closeValueStock,
                f.highValueStock,
                f.lowValueStock,
                f.quantityStock
            FROM stagingFactStocks f
            JOIN dimTime t ON f.keyTime = t.keyTime
            JOIN dimCompany c ON f.keyCompany = c.keyCompany
            """)
            
            # Menghitung jumlah baris yang dimuat
            coin_count = self.conn.execute("SELECT COUNT(*) FROM dimCoin").fetchone()[0]
            company_count = self.conn.execute("SELECT COUNT(*) FROM dimCompany").fetchone()[0]
            time_count = self.conn.execute("SELECT COUNT(*) FROM dimTime").fetchone()[0]
            fact_coin_count = self.conn.execute("SELECT COUNT(*) FROM factCoins").fetchone()[0]
            fact_stock_count = self.conn.execute("SELECT COUNT(*) FROM factStocks").fetchone()[0]

            total_records = coin_count + company_count + time_count + fact_coin_count + fact_stock_count

            self.log_process_end(log_id, records=total_records, status='SUCCESS', 
                message=f'Loaded {coin_count} coins, {company_count} companies, '
                        f'{time_count} time records, {fact_coin_count} coin facts, '
                        f'{fact_stock_count} stock facts')
            return True
            
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
    
    def delete_staging_table_value(self):
        self.conn.execute("DELETE FROM stagingFactCoins")
        self.conn.execute("DELETE FROM stagingFactStocks")
        self.conn.execute("DELETE FROM stagingDimTime")
        self.conn.execute("DELETE FROM stagingDimCoin")
        self.conn.execute("DELETE FROM stagingDimCompany")
    
    def insert_fact_data(self, csv_path, staging_fact_table, dim_table, key_column, dimension_lookup_column):
        log_id = self.log_process_start("insert_fact_data")
        
        try:
            # Check if CSV file exists
            import os
            if not os.path.exists(csv_path):
                print(f"⚠️ CSV file {csv_path} does not exist. Skipping process.")
                self.log_process_end(log_id, status='SUCCESS', message=f'CSV file {csv_path} does not exist. Process skipped.')
                return True
                
            self.delete_staging_table_value()  # optional cleanup if you have this implemented
            
            # Load CSV
            df = pd.read_csv(csv_path, parse_dates=['time'])

            current_key_time = self.conn.execute("SELECT COALESCE(MAX(keyTime), 0) FROM dimTime").fetchone()[0]

            for _, row in df.iterrows():
                time = row['time']
                lookup_value = row[dimension_lookup_column]

                # Extract all fact columns (3rd column onward)
                fact_columns = df.columns[2:]
                fact_values = [row[col] for col in fact_columns]

                # Check time in dimTime
                time_row = self.conn.execute("SELECT keyTime FROM dimTime WHERE datetime = ?", (time.strftime('%Y-%m-%d'),)).fetchone()

                if time_row:
                    key_time_id = time_row[0]
                else:
                    # Generate new keyTime
                    current_key_time += 1
                    day = time.day
                    day_of_week = time.weekday() + 1
                    day_abbrev = time.strftime('%a').upper()
                    day_full = time.strftime('%A').upper()
                    month = time.month
                    month_abbrev = time.strftime('%b').upper()
                    month_full = time.strftime('%B').upper()
                    bimonth = ((month - 1) // 2) + 1
                    quarter = ((month - 1) // 3) + 1
                    semester = 1 if month <= 6 else 2
                    year = time.year

                    # Insert into dimTime or stagingDimTime if you prefer
                    self.conn.execute("""
                        INSERT INTO dimTime (
                            keyTime, datetime, dayTime, dayWeekTime, dayWeekAbbrevTime,
                            dayWeekCompleteTime, monthTime, monthAbbrevTime,
                            monthCompleteTime, bimonthTime, quarterTime,
                            semesterTime, yearTime
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        current_key_time, time.date(), day, day_of_week, day_abbrev,
                        day_full, month, month_abbrev, month_full,
                        bimonth, quarter, semester, year
                    ))
                    key_time_id = current_key_time

                # Lookup dimension key
                key_row = self.conn.execute(f"SELECT {key_column} FROM {dim_table} WHERE {dimension_lookup_column} = ?", (lookup_value,)).fetchone()

                if not key_row:
                    print(f"⚠️ Value '{lookup_value}' not found in {dim_table}. Skipping row.")
                    continue

                key_value = key_row[0]

                # Insert into staging fact table dynamically
                column_names = ['keyTime', key_column] + list(fact_columns)
                placeholders = ', '.join(['?' for _ in column_names])
                insert_sql = f"""
                    INSERT INTO {staging_fact_table} ({', '.join(column_names)}) 
                    VALUES ({placeholders})
                """
                self.conn.execute(insert_sql, (key_time_id, key_value, *fact_values))

            print(f"✅ Data successfully inserted into {staging_fact_table}.")
            
            # Delete the CSV file after successful processing
            os.remove(csv_path)
            print(f"✅ CSV file {csv_path} has been deleted after successful processing.")
            
            self.log_process_end(log_id, status='SUCCESS', message=f'Fact data has been inserted and CSV file {csv_path} has been deleted')
            return True
            
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
        
    def upsert_scd_type2(self, table_name, key_column, key_id, new_data: list[dict]):
        """
        Slowly Changing Dimension Type 2 Upsert Logic

        Parameters:
        - table_name (str): target dimension table name (e.g., 'dimCompany')
        - key_column (str): primary key column name (e.g., 'keyCompany')
        - new_data (list[dict]): new data to insert or update
        """
        
        log_id = self.log_process_start("upsert_scd_type2")
        try:
            for record in new_data:
                key = record[key_column]

                # Check if an active record exists for the given key
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE {key_column} = ? AND isActive = TRUE
                """
                existing = self.conn.execute(query, [key]).fetchone()
                max_key_coin = self.conn.execute(f"SELECT MAX({key_id}) FROM {table_name}").fetchone()[0]

                if existing:
                    # Compare fields excluding key, startedAt, endedAt, isActive
                    update_needed = False
                    columns_info = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
                    column_names = [col[1] for col in columns_info]

                    for col in record:
                        if col in [key_column, 'startedAt', 'endedAt', 'isActive']:
                            continue

                        old_value = existing[column_names.index(col)]
                        if record[col] != old_value:
                            print(f"Column {col} need to update")
                            update_needed = True
                            break

                    if update_needed:
                        # Mark old record as inactive
                        self.conn.execute(f"""
                            UPDATE {table_name}
                            SET isActive = FALSE, endedAt = ?
                            WHERE {key_column} = ? AND isActive = TRUE
                        """, [datetime.now(), key])

                        # Insert new record with startedAt = now, endedAt = NULL, isActive = TRUE
                        columns = ', '.join(column_names)
                        placeholders = ', '.join(['?'] * len(column_names))
                        values = [max_key_coin + 1] + list(record.values()) + [datetime.now(), None,  True]

                        self.conn.execute(f"""
                            INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                        """, values)

                        print(f"✅ Value {key_column} from table {table_name} has been updated with {record}")

                else:
                    # Insert as a new record
                    columns = ', '.join(column_names)
                    placeholders = ', '.join(['?'] * len(column_names))
                    values = [max_key_coin + 1] + list(record.values()) + [datetime.now(), None,  True]

                    self.conn.execute(f"""
                        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                    """, values)
                    print(f"✅ Value {key_column} from table {table_name} has been added with {record}")
                
            self.log_process_end(log_id, status='SUCCESS', message='Upsert success')
        except Exception as e:
            self.log_process_end(log_id, status='ERROR', message=str(e))
            raise
                
    
    def run_full_pipeline(self):
        """Jalankan seluruh pipeline ETL"""
        print("Menjalankan ETL Pipeline lengkap...")
        
        start_time = datetime.now()
        
        try:                
            # 1. Inisialisasi warehouse
            self.initialize_warehouse()
            print("✅ Data warehouse diinisialisasi")
            
            # 2. Ekstrak data
            self.extract_all_sources()
            print("✅ Data diekstrak dari semua sumber")
            
            # 3. Transform data
            self.transform_data()
            print("✅ Data ditransformasi")
            
            # 4. Load data
            self.delete_tables_data()
            self.load_data()
            print("✅ Data dimuat ke data warehouse")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"\nPipeline ETL selesai dalam {duration:.2f} detik")
            
            # Menampilkan statistik
            stats = self.conn.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM dimCoin) as coin_count,
                    (SELECT COUNT(*) FROM dimCompany) as company_count,
                    (SELECT COUNT(*) FROM dimTime) as time_count,
                    (SELECT COUNT(*) FROM factCoins) as fact_coin_count,
                    (SELECT COUNT(*) FROM factStocks) as fact_stock_count
            """).fetchone()

            print("\nStatistik Data Warehouse:")
            print(f"✅ Dimensi Coin: {stats[0]} baris")
            print(f"✅ Dimensi Company: {stats[1]} baris")
            print(f"✅ Dimensi Time: {stats[2]} baris")
            print(f"✅ Fakta Coin: {stats[3]} baris")
            print(f"✅ Fakta Stock: {stats[4]} baris")
            
            return True
            
        except Exception as e:
            print(f"ERROR: Pipeline ETL gagal: {str(e)}")
            self.conn.close()
            print(f"Database Closed")
            return False
    
    def get_etl_log(self, limit=10):
        """Menampilkan log ETL terakhir"""
        return self.conn.execute(f"""
            SELECT 
                logId,
                processName,
                startTime,
                endTime,
                EXTRACT(EPOCH FROM (endTime - startTime)) as duration_seconds,
                recordsProcessed,
                status,
                message
            FROM etlLog
            ORDER BY logId DESC
            LIMIT {limit}
        """).fetchdf()
    
    def __del__(self):
        """Menutup koneksi saat objek dihapus"""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
                print("Koneksi DuckDB ditutup.")
        except:
            pass
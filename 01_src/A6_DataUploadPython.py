"""
GRUPPO 13 - Assignment 6: Data Uploading
Descrizione: Script finale per il caricamento dei dati nel Data Warehouse.            
            Gestisce le differenze tra tabelle con chiave primaria manuale (inserimento esplicito)
            e tabelle con chiave IDENTITY (salto della colonna PK del CSV).
"""

import csv
import pyodbc
from pathlib import Path

# ==============================================================================
# 0. CONFIGURAZIONE CONNESSIONE (DATABASE CONNECTION)
# ==============================================================================

SERVER = "131.114.50.57"
DATABASE = "Group_ID_13_DB"
USERNAME = "Group_ID_13"
PASSWORD = "9F7LUZXT"

# Costruzione stringa di connessione ODBC
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
)

# Definizione path di input (cartella output dello script A5)
BASE_DIR = Path(__file__).resolve().parents[1] / "00_data" / "warehouse_ready"

print("\n=========== STARTING ASSIGNMENT 6: DATA UPLOAD ===========\n")

# Apertura connessione
try:
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    print(f"[INFO] Connected to {SERVER}/{DATABASE}")
except Exception as e:
    print(f"[FATAL] Connection error: {e}")
    exit()


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def load_csv(name):
    """
    Legge il file CSV specificato e restituisce header e lista righe.
    """
    path = BASE_DIR / name
    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        return [], []
        
    with open(path, "r", encoding="utf8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            rows = list(reader)
            return header, rows
        except StopIteration:
            return [], []


# ==============================================================================
# 1. CARICAMENTO DIMENSIONI (LOOKUP TABLES)
# ==============================================================================

# --- DimDate ---
# PK: Manuale (Key YYYYMMDD) -> Inseriamo l'intera riga 'r'
print("[1/10] Uploading DimDate...")
_, rows = load_csv("DimDate.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimDate (date_song_pk, full_date, day, month, year, weekday, season)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, r)
conn.commit()


# --- DimGeography ---
# PK: Manuale -> Inseriamo l'intera riga 'r'
print("[2/10] Uploading DimGeography...")
_, rows = load_csv("DimGeography.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimGeography (geo_pk, birth_place, province, region, country)
        VALUES (?, ?, ?, ?, ?)
    """, r)
conn.commit()


# --- DimCategory ---
# PK: IDENTITY (Auto-increment) -> Usiamo r[1:] per saltare la PK presente nel CSV
print("[3/10] Uploading DimCategory...")
_, rows = load_csv("DimCategory.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimCategory (category_name)
        VALUES (?)
    """, r[1:]) 
conn.commit()


# --- DimArtist ---
# PK: IDENTITY -> Usiamo r[1:] per saltare la PK del CSV
print("[4/10] Uploading DimArtist...")
_, rows = load_csv("DimArtist.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimArtist (id_artist, geo_fk, name, gender, birth_date, nationality, description)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, r[1:]) 
conn.commit()


# --- DimAudio ---
# PK: Manuale -> Inseriamo l'intera riga 'r'
print("[5/10] Uploading DimAudio...")
_, rows = load_csv("DimAudio.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimAudio (
            audio_pk, bpm, loudness, flatness, rolloff,
            flux, rms, spectral_complexity, pitch
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, r)
conn.commit()


# --- DimLyrics ---
# PK: Manuale -> Inseriamo l'intera riga 'r'
print("[6/10] Uploading DimLyrics...")
_, rows = load_csv("DimLyrics.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimLyrics (
            lyrics_pk, n_tokens, n_sentences,
            avg_token_per_clause, char_per_tok,
            swear_IT, swear_EN
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, r)
conn.commit()


# ==============================================================================
# 2. CARICAMENTO DIMENSIONI PRINCIPALI & BRIDGE
# ==============================================================================

# --- DimTrack ---
# PK: IDENTITY -> Usiamo r[1:] per saltare la PK del CSV
print("[7/10] Uploading DimTrack...")
_, rows = load_csv("DimTrack.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO DimTrack (
            id_track, title, duration_ms, explicit,
            track_number, disc_number, original_source_id,
            album_release_date, audio_fk, lyrics_fk
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, r[1:]) 
conn.commit()


# --- BridgeFeatured ---
# Tabella Bridge (PK Composta Manuale) -> Inseriamo l'intera riga 'r'
print("[8/10] Uploading BridgeFeatured...")
_, rows = load_csv("BridgeFeatured.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO BridgeFeatured (track_fk, artist_fk, role)
        VALUES (?, ?, ?)
    """, r)
conn.commit()


# ==============================================================================
# 3. CARICAMENTO FACT TABLE
# ==============================================================================

# --- FactSongStreams ---
# Nota: La Fact ha PK Identity su SQL, ma il CSV generato in A5 non contiene 
# la colonna PK, inizia direttamente con le FK. Quindi usiamo 'r' intero.
print("[9/10] Uploading FactSongStreams...")
_, rows = load_csv("FactSongStreams.csv")
for r in rows:
    cursor.execute("""
        INSERT INTO FactSongStreams (
            date_song_fk, track_fk, category_fk,
            streams_1month, popularity
        ) VALUES (?, ?, ?, ?, ?)
    """, r)
conn.commit()


# ==============================================================================
# CHIUSURA E CLEANUP
# ==============================================================================

print("\n=========== DATA UPLOAD COMPLETED SUCCESSFULLY ===========\n")

cursor.close()
conn.close()
"""
GRUPPO 13 - Assignment 1: Data Understanding
Descrizione: Script per l'analisi esplorativa preliminare dei dataset (JSON e XML).
"""

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import Counter

# ==============================================================================
# CONFIGURAZIONE PERCORSI E AMBIENTE
# Utilizziamo percorsi relativi per garantire la portabilità del progetto 
# su diverse macchine senza modificare il codice.
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "00_data" / "raw"

TRACKS_PATH = RAW_DIR / "tracks.json"
ARTISTS_PATH = RAW_DIR / "artists.xml"


# ==============================================================================
# SEZIONE 1: CARICAMENTO DATI 
# ==============================================================================

def load_tracks():
    """
    Carica il dataset delle tracce dal file JSON.
    Gestisce la lettura con encoding UTF-8 per supportare caratteri speciali.
    """
    print(f"[INFO] Loading tracks from: {TRACKS_PATH}")
    try:
        with open(TRACKS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[INFO] Loaded {len(data)} tracks.")
        return data
    except FileNotFoundError:
        print(f"[ERROR] File non trovato: {TRACKS_PATH}")
        return []


def load_artists():
    """
    Effettua il parsing del file XML degli artisti.
    Restituisce una lista di elementi <row> pronti per l'estrazione.
    """
    print(f"[INFO] Loading artists from: {ARTISTS_PATH}")
    try:
        tree = ET.parse(ARTISTS_PATH)
        root = tree.getroot()
        
        # Gli artisti sono incapsulati nei tag <row> all'interno della root <data>
        artist_list = list(root.findall("./row"))
        print(f"[INFO] Loaded {len(artist_list)} artists.")
        return artist_list
    except FileNotFoundError:
        print(f"[ERROR] File non trovato: {ARTISTS_PATH}")
        return []


# ==============================================================================
# SEZIONE 2: ANALISI STRUTTURA DATI 
# ==============================================================================

def explore_tracks_structure(tracks):
    """
    Analizza lo schema del JSON.
    1. Ispeziona il primo elemento per determinare i campi attesi.
    2. Calcola la frequenza di ogni chiave nell'intero dataset per 
       verificare se lo schema è consistente (tutti i record hanno gli stessi campi).
    """
    print("\n=== TRACKS STRUCTURE ANALYSIS ===")
    
    if not tracks:
        print("No tracks found.")
        return

    # Campionamento del primo record per visualizzare la struttura base
    first = tracks[0]
    print("Fields found in the first track:")
    for key in first.keys():
        print(f" - {key}")

    # Verifica di consistenza: contiamo la presenza delle chiavi su tutti i record
    key_counter = Counter()
    for t in tracks:
        key_counter.update(t.keys())

    total = len(tracks)
    print("\nField frequency across the dataset:")
    for key, count in key_counter.items():
        # Calcolo percentuale di presenza del campo
        perc = (count / total) * 100
        print(f"{key}: {count}/{total} ({perc:.1f}%)")


def explore_artists_structure(artists):
    """
    Analizza la struttura gerarchica del file XML.
    """
    print("\n=== ARTISTS STRUCTURE ANALYSIS ===")
    if not artists:
        return

    first = artists[0]
    print(f"Element tag: {first.tag}")
    
    # Estrazione dei tag figli per capire quali colonne sono disponibili
    children = [child.tag for child in first]
    print("Child tags:")
    for tag in children:
        print(f" - {tag}")


# ==============================================================================
# SEZIONE 3: MISSING VALUE ANALYSIS
# ==============================================================================

def count_missing_values_tracks(tracks, fields):
    """
    Analisi di completezza per il dataset Tracks.
    Consideriamo 'missing' i valori: None, stringhe vuote, liste vuote o dizionari vuoti.
    """
    print("\n=== MISSING VALUES (TRACKS) ===")
    total = len(tracks)
    
    for field in fields:
        missing = 0
        for t in tracks:
            val = t.get(field)
            # Data Quality Check: Identifica come "mancante" non solo il valore None, 
            # ma anche stringhe vuote ("") o strutture dati vuote (liste [] o dict {})
            # tipiche di campi opzionali nel formato JSON.
            if val in (None, "", [], {}):
                missing += 1
        
        perc = (missing / total) * 100
        print(f"{field}: {missing} missing ({perc:.1f}%)")


def get_artist_text(artist_element, tag_name):
    """
    Funzione helper per estrarre testo dai nodi XML in sicurezza.
    Gestisce i casi di nodi mancanti o vuoti.
    """
    node = artist_element.find(tag_name)
    if node is None:
        return None
    text = node.text
    # Normalizza stringhe vuote o composte solo da spazi
    if text is None or not text.strip():
        return None
    return text.strip()


def count_missing_values_artists(artists, fields):
    """
    Analisi di completezza per il dataset Artisti (XML).
    Utilizza l'helper get_artist_text per gestire le peculiarità dell'XML.
    """
    print("\n=== MISSING VALUES (ARTISTS) ===")
    total = len(artists)
    
    for field in fields:
        missing = 0
        for a in artists:
            if get_artist_text(a, field) is None:
                missing += 1
        
        perc = (missing / total) * 100
        print(f"{field}: {missing} missing ({perc:.1f}%)")


# ==============================================================================
# SEZIONE 4: CHECK DUPLICATI E CHIAVI PRIMARIE
# ==============================================================================

def check_id_duplicates_tracks(tracks):
    """
    Verifica primaria: controlla se l'attributo 'id' è univoco nel dataset.
    Identifica duplicati tecnici esatti.
    """
    print("\n=== ID DUPLICATES (TRACKS) ===")
    seen = set()
    duplicates = 0
    
    for t in tracks:
        tid = t.get("id")
        if tid is None:
            continue
            
        if tid in seen:
            duplicates += 1
        else:
            seen.add(tid)
            
    print(f"Number of duplicate IDs found: {duplicates}")


def check_id_duplicates_artists(artists):
    """
    Verifica l'univocità della chiave primaria 'id_author' nel dataset Artisti.
    """
    print("\n=== ID DUPLICATES (ARTISTS) ===")
    seen = set()
    duplicates = 0
    
    for a in artists:
        aid = get_artist_text(a, "id_author")
        if aid is None:
            continue 
            
        if aid in seen:
            duplicates += 1
        else:
            seen.add(aid)
            
    print(f"Number of duplicate IDs found: {duplicates}")


def find_real_duplicates(tracks):
    """
    Analisi avanzata delle collisioni (Real Duplicates).
    Verifica se lo stesso ID è stato assegnato erroneamente a 
    coppie (Titolo, Artista) diverse.
    Per decidere se utilizzare o meno la PK fornita.
    """
    print("\n=== REAL ID COLLISIONS ===")
    
    # Mappa: ID -> Lista di tuple uniche (Titolo, Artista)
    content_map = {}
    collisions = []

    for t in tracks:
        tid = t.get("id")
        # Identificativo semantico del brano
        info = (t.get("title"), t.get("primary_artist"))
        
        if tid not in content_map:
            content_map[tid] = []
        content_map[tid].append(info)

    # Logica di rilevamento: se un ID punta a più contenuti diversi, è una collisione
    for tid, info_list in content_map.items():
        unique_elements = set(info_list)
        
        if len(unique_elements) > 1:
            collisions.append((tid, unique_elements))

    if not collisions:
        print("No ID collisions found.")
    else:
        print(f"Found {len(collisions)} IDs used improperly for different songs!")
        # Stampa i primi 5 esempi per evidenza nel report
        for i, (tid, songs) in enumerate(collisions[:5]):
            print(f"- ID {tid} used for: {songs}")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    # 1. Caricamento dei dati dalle sorgenti raw
    tracks = load_tracks()
    artists = load_artists()

    # 2. Esplorazione della struttura (Schema Check)
    explore_tracks_structure(tracks)
    explore_artists_structure(artists)

    # 3. Analisi dei valori mancanti (Data Quality Assessment)
    # Selezione dei campi rilevanti per l'analisi
    track_fields = [
        "title", "primary_artist", "featured_artists", "language",
        "year", "month", "day", "lyrics", "streams@1month",
        "album_name", "album_release_date"
    ]
    count_missing_values_tracks(tracks, track_fields)

    artist_fields = [
        "id_author", "name", "gender", "birth_date", "birth_place",
        "nationality", "active_start", "active_end",
        "province", "region", "country", "latitude", "longitude"
    ]
    count_missing_values_artists(artists, artist_fields)

    # 4. Verifica Integrità e Duplicati
    check_id_duplicates_tracks(tracks)
    check_id_duplicates_artists(artists)
    
    # Controllo critico: ID Collision
    find_real_duplicates(tracks)

if __name__ == "__main__":
    main()
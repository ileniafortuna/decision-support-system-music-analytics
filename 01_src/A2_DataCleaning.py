"""
GRUPPO 13 - Assignment 2: Data Cleaning
Descrizione: Pipeline di pulizia, standardizzazione e arricchimento dei dati.

# ==============================================================================
# NOTA SUL FLUSSO DI ESECUZIONE
# ==============================================================================
# Questo script genera il file intermedio '00_data/cleaned/artists_cleaned.json'.
# Sebbene tale file sia tecnicamente pronto per il DW, contiene ancora molti campi 
# "Unknown" (luogo di nascita, date, regioni).
#
# PER UN RISULTATO OTTIMALE:
# Eseguire successivamente lo script '00_data/utilities/A2b_data_enrichment.py'.
# Tale modulo interroga le API di Wikidata per recuperare le informazioni mancanti
# e arricchire la dimensione geografica degli artisti.
# ==============================================================================
"""

import json
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from collections import Counter

# Gestione dipendenze opzionali: tentiamo l'import di Shapely per l'arricchimento
# geografico (point-in-polygon). Se fallisce, il processo continua senza geo-features.
try:
    from shapely.geometry import shape, Point
    import json as json_lib 
    GEO_AVAILABLE = True
except ImportError:
    GEO_AVAILABLE = False
    print("[NOTE] 'shapely' library not found. Skipping advanced geo-enrichment.")

# ==============================================================================
# CONFIGURAZIONE PERCORSI E AMBIENTE
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "00_data" / "raw"
CLEANED_DIR = BASE_DIR / "00_data" / "cleaned"
EXTERNAL_DIR = BASE_DIR / "00_data" / "external"

# Creazione automatica directory output se non esiste
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# FUNZIONI DI PULIZIA E TRASFORMAZIONE DATI 
# ==============================================================================

def clean_text(text):
    """
    Normalizza le stringhe rimuovendo whitespace, newline e caratteri sporchi.
    Restituisce None se la stringa risultante è vuota.
    """
    if not text: return None
    s = str(text).replace("\n", " ").replace("\r", " ").strip()
    return " ".join(s.split()) 

def clean_number(value, type_func=float, min_val=None, max_val=None):
    """
    Effettua il casting sicuro a numerico, applicando controlli di range (Data Validation).
    Restituisce None se il valore è fuori dai limiti o non convertibile.
    """
    if value in (None, "", "NaN"): return None
    try: 
        num = type_func(value)
        if min_val is not None and num < min_val: return None
        if max_val is not None and num > max_val: return None
        return num
    except: 
        return None

def round_val(value, decimals):
    """Arrotondamento sicuro per valori float."""
    if value is None: return None
    try: return round(float(value), decimals)
    except: return None

def parse_date(date_str):
    """
    Tenta il parsing di date stringa supportando formati multipli eterogenei.
    Restituisce un oggetto datetime.date.
    """
    if not date_str: return None
    formats = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"]
    for fmt in formats:
        try: return datetime.strptime(date_str, fmt).date()
        except: continue
    return None

def get_season(month):
    """Deriva la stagione meteorologica basandosi sul mese di rilascio."""
    if month in [12, 1, 2]: return "winter"
    if month in [3, 4, 5]: return "spring"
    if month in [6, 7, 8]: return "summer"
    return "autumn"

def parse_list(value):
    """
    Deserializza stringhe che rappresentano liste (es. "['a', 'b']") in liste Python.
    Gestisce formati non standard.
    """
    if not value or value == "[]": return []
    if isinstance(value, list): return value
    cleaned = value.strip("[]")
    return [item.strip(" '\"") for item in cleaned.split(",") if item.strip()]

def to_bool(value):
    """Normalizza valori booleani eterogenei (1, 'true', 'yes') a booleani Python."""
    if isinstance(value, bool): return value
    s = str(value).lower()
    return s in ["1", "true", "yes", "t"]

# ==============================================================================
# MAIN PIPELINE 
# ==============================================================================
def main():
    print("=== START DATA CLEANING PIPELINE ===")

    # 1. CARICAMENTO DATI RAW (DATA INGESTION)
    print("Loading raw files...")
    with open(RAW_DIR / "tracks.json", "r", encoding="utf-8") as f:
        raw_tracks = json.load(f)
    
    # Parsing XML per gli artisti
    tree = ET.parse(RAW_DIR / "artists.xml")
    raw_artists = list(tree.getroot().findall("./row"))

    # Caricamento poligoni GeoJSON per spatial join (Province Italiane)
    province_polygons = {}
    path_geo = EXTERNAL_DIR / "italy_provinces.geojson"
    if GEO_AVAILABLE and path_geo.exists():
        with open(path_geo, "r", encoding="utf-8") as f:
            geo_data = json_lib.load(f)
        for feat in geo_data["features"]:
            name = feat["properties"].get("prov_name")
            if name: province_polygons[name] = shape(feat["geometry"])

    # ----------------------------------------------------
    # 2. PULIZIA E ARRICCHIMENTO ARTISTI
    # ----------------------------------------------------
    print("Cleaning Artists...")
    cleaned_artists = []
    # Mappa di fallback per regioni note (Data Enrichment manuale)
    region_map = {"Milano": "Lombardia", "Roma": "Lazio", "Napoli": "Campania", "Torino": "Piemonte"}

    for node in raw_artists:
        a = {}
        for child in node:
            a[child.tag] = child.text.strip() if child.text else None
        
        # Pulizia campi testuali (Handling Missing Values con placeholder 'Unknown')
        a["name"] = clean_text(a.get("name")) or "Unknown Artist"
        a["birth_place"] = clean_text(a.get("birth_place")) or "Unknown"
        a["nationality"] = clean_text(a.get("nationality")) or "Unknown"
        a["birth_date"] = clean_text(a.get("birth_date")) or "Unknown"
        a["gender"] = clean_text(a.get("gender")) or "Unknown"
        a["description"] = clean_text(a.get("description")) or "Unknown"
        
        # Enrichment Geografico: Reverse Geocoding via coordinate
        lat = clean_number(a.get("latitude"))
        lon = clean_number(a.get("longitude"))
        
        found_prov = None
        if GEO_AVAILABLE and lat and lon:
            p = Point(lon, lat)
            for prov_name, poly in province_polygons.items():
                if poly.contains(p):
                    found_prov = prov_name
                    break
        
        # Logica di assegnazione gerarchica: Polygon -> Map -> Unknown
        a["province"] = clean_text(a.get("province")) or found_prov or "Unknown"
        a["region"] = clean_text(a.get("region")) or region_map.get(a["province"], "Unknown")
        a["country"] = "Italy" if a["province"] != "Unknown" else "Unknown"

        # Rimozione colonne tecniche non necessarie per il DW
        a.pop("latitude", None); a.pop("longitude", None)
        a.pop("active_start", None); a.pop("active_end", None)
        cleaned_artists.append(a)

    # ----------------------------------------------------
    # 3. PULIZIA E STANDARDIZZAZIONE TRACCE
    # ----------------------------------------------------
    print("Cleaning Tracks...")
    
    # NORMALIZZAZIONE NOMI ALBUM
    # Usiamo un approccio a maggioranza (Moda) per correggere nomi album inconsistenti
    album_names_map = {}
    for t in raw_tracks:
        aid = t.get("id_album")
        aname = t.get("album_name") or t.get("album")
        if aid and aname:
            if aid not in album_names_map: album_names_map[aid] = []
            album_names_map[aid].append(clean_text(aname))
    
    official_album_names = {
        aid: Counter(names).most_common(1)[0][0] 
        for aid, names in album_names_map.items() if names
    }

    cleaned_tracks = []
    valid_bpms = [] 

    for t in raw_tracks:
        new_t = t.copy()
        
        # Applicazione fix nome album
        aid = t.get("id_album")
        correct_name = official_album_names.get(aid, clean_text(t.get("album_name") or t.get("album")) or "Unknown")
        
        # GESTIONE DATE (Logica di Imputazione e Derivazione)
        # Priorità: Data Specifica Traccia -> Data Rilascio Album -> Null
        final_date = None
        
        # Tentativo 1: Data track (Y,M,D atomici)
        y, m, d = t.get("year"), t.get("month"), t.get("day")
        if y and m and d:
            try: final_date = datetime(int(float(y)), int(float(m)), int(float(d))).date()
            except: pass
        
        # Tentativo 2: Data album
        if not final_date:
            final_date = parse_date(t.get("album_release_date"))
            
        if final_date:
            new_t["year"], new_t["month"], new_t["day"] = final_date.year, final_date.month, final_date.day
            new_t["release_season"] = get_season(final_date.month)
            new_t["release_weekday"] = final_date.weekday()
            new_t["full_date"] = final_date.strftime("%Y-%m-%d")
        else:
            # Dati temporali insufficienti
            new_t["release_season"] = None
            new_t["release_weekday"] = None
            new_t["full_date"] = None 

        # Standardizzazione formato data album (solo display)
        original_alb_date = parse_date(t.get("album_release_date"))
        if original_alb_date:
            new_t["album_release_date"] = original_alb_date.strftime("%Y-%m-%d")
        else:
            new_t["album_release_date"] = None

        # Pulizia Metriche Numeriche e Arrotondamento
        bpm = clean_number(t.get("bpm"), float, min_val=40, max_val=260)
        if bpm:
            new_t["bpm"] = round_val(bpm, 2)
            valid_bpms.append(bpm)
        else:
            new_t["bpm"] = None 
            
        new_t["streams@1month"] = clean_number(t.get("streams@1month"), int)
        new_t["popularity"] = clean_number(t.get("popularity"), int, min_val=0, max_val=100)
        new_t["duration_ms"] = clean_number(t.get("duration_ms"), int)
        new_t["disc_number"] = clean_number(t.get("disc_number"), int)
        new_t["track_number"] = clean_number(t.get("track_number"), int)
        new_t["n_sentences"] = clean_number(t.get("n_sentences"), int)
        new_t["n_tokens"] = clean_number(t.get("n_tokens"), int)
        
        # Feature audio (arrotondate a 2 decimali)
        audio_cols = ["loudness", "flatness", "rolloff", "flux", "rms", "spectral_complexity", "pitch"]
        for col in audio_cols: new_t[col] = round_val(clean_number(t.get(col)), 2)

        # Metriche testuali (arrotondate a 3 decimali)
        text_cols = ["avg_token_per_clause", "char_per_tok"]
        for col in text_cols: new_t[col] = round_val(clean_number(t.get(col)), 3)

        # Pulizia Testi e Liste
        new_t["title"] = clean_text(t.get("title"))
        new_t["lyrics"] = clean_text(t.get("lyrics")) or "Unknown"
        new_t["explicit"] = 1 if to_bool(t.get("explicit")) else 0
        new_t["swear_IT_words"] = parse_list(t.get("swear_IT_words"))
        new_t["swear_EN_words"] = parse_list(t.get("swear_EN_words"))
        
        new_t["swear_IT"] = clean_number(t.get("swear_IT"), int)
        new_t["swear_EN"] = clean_number(t.get("swear_EN"), int)
        
        # Normalizzazione Featured Artists
        feats = t.get("featured_artists")
        if isinstance(feats, str):
            new_t["featured_artists"] = [f.strip() for f in feats.split(',') if f.strip()]
        elif isinstance(feats, list):
            new_t["featured_artists"] = [str(f).strip() for f in feats if str(f).strip()]
        else:
            new_t["featured_artists"] = []

        # Rimozione colonne ridondanti
        new_t.pop("language", None)
        new_t.pop("album", None)
        new_t.pop("album_name", None)

        # Riempimento placeholder 'Unknown' solo per campi testuali sicuri
        text_fields_to_fill = ["title", "lyrics", "original_source_id", "compilation_name"]
        
        for k in text_fields_to_fill:
            if new_t.get(k) is None:
                new_t[k] = "Unknown"
        
        cleaned_tracks.append(new_t)

    # Imputazione BPM mancanti (Mean)
    avg_bpm = sum(valid_bpms)/len(valid_bpms) if valid_bpms else 120.0
    print(f"Average BPM calculated: {avg_bpm:.2f}")
    
    for t in cleaned_tracks:
        if t["bpm"] is None: t["bpm"] = round(avg_bpm, 2)

    # 4. DEDUPLICAZIONE E INTEGRITÀ
    print("Deduplication...")
    final_tracks = []
    seen = set()
    for t in cleaned_tracks:
        if not t.get("title"): continue
        # Firma univoca brano: (Titolo, Artista)
        sig = (str(t["title"]).lower().strip(), str(t.get("id_artist")).strip())
        if sig in seen: continue
        seen.add(sig)
        
        # Generazione UUID per chiave primaria robusta
        t["original_source_id"] = t.get("id")
        t["id"] = str(uuid.uuid4())
        final_tracks.append(t)

    # 5. SALVATAGGIO OUTPUT
    print(f"Saving: {len(final_tracks)} tracks, {len(cleaned_artists)} artists.")
    with open(CLEANED_DIR / "tracks_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(final_tracks, f, indent=2, ensure_ascii=False)
    with open(CLEANED_DIR / "artists_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned_artists, f, indent=2, ensure_ascii=False)

    print("Pipeline completed.")

if __name__ == "__main__":
    main()
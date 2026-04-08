"""
GRUPPO 13 - Assignment 5: Data Preparation
Descrizione: Generazione delle tabelle fisiche (CSV) per il Data Warehouse.
Obiettivo:  Normalizzare i dataset arricchiti suddividendoli in
            Dimensioni, Bridge e Fact Table. Generazione delle chiavi surrogate (PK)
            e gestione delle relazioni (FK) per lo schema.

Input:  00_data/enriched/artists_enriched.csv
        00_data/enriched/tracks_A3.csv
Output: 00_data/warehouse_ready/*.csv (vedi specifiche tabelle sotto)
"""

import csv
from pathlib import Path

# ==============================================================================
# CONFIGURAZIONE AMBIENTE
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENRICHED_DIR = BASE_DIR / "00_data" / "enriched"
WAREHOUSE_DIR = BASE_DIR / "00_data" / "warehouse_ready"

# Creazione cartella di output se non esiste
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# FUNZIONI HELPER (CSV I/O)
# ==============================================================================

def load_csv(path):
    """
    Legge un CSV e restituisce header e lista di righe.
    Utilizza encoding UTF-8 per caratteri speciali.
    """
    try:
        with open(path, "r", encoding="utf8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            return [], []
        return rows[0], rows[1:]
    except FileNotFoundError:
        print(f"[ERROR] File non trovato: {path}")
        return [], []

def save_csv(path, header, rows):
    """
    Scrive una lista di liste in un file CSV.
    Quote minimali per ridurre la dimensione del file.
    """
    try:
        with open(path, "w", encoding="utf8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            writer.writerows(rows)
        print(f"[OK] Generated: {path.name}")
    except IOError as e:
        print(f"[ERROR] Impossibile scrivere {path}: {e}")

# ==============================================================================
# PIPELINE DI PREPARAZIONE
# ==============================================================================

def main():
    print("=== START A5 PIPELINE: WAREHOUSE PREPARATION ===")

    # 1. CARICAMENTO DATI STAGING
    print("[INFO] Loading enriched datasets...")
    artists_header, artists = load_csv(ENRICHED_DIR / "artists_enriched.csv")
    tracks_header, tracks = load_csv(ENRICHED_DIR / "tracks_A3.csv")

    if not artists or not tracks:
        print("[ERROR] Dati mancanti. Interruzione.")
        return

    # Creazione mappe indice colonna (per accesso rapido via nome)
    a_idx = {c: i for i, c in enumerate(artists_header)}
    t_idx = {c: i for i, c in enumerate(tracks_header)}

    artist_id_col = "id_author"

    # Lookup Table: Nome Artista -> ID Originale (per risolvere i Featured Artists)
    name_to_id = {
        a[a_idx["name"]].strip().lower(): a[a_idx[artist_id_col]]
        for a in artists
    }

    # ==========================================================================
    # 2. GENERAZIONE DIMENSIONI (LOOKUP TABLES)
    # ==========================================================================

    # --- 2.1 DIM GEOGRAPHY ---
    # Aggregazione luoghi unici. PK=0 riservata per valori completamente mancanti.
    print("[INFO] Generating DimGeography...")
    
    geo_dict = {("Unknown", "Unknown", "Unknown", "Unknown"): 0}
    geo_pk = 1

    for a in artists:
        key = (
            a[a_idx["birth_place"]],
            a[a_idx["province"]],
            a[a_idx["region"]],
            a[a_idx["country"]]
        )

        # Skip se è il placeholder "Unknown" (già mappato a 0)
        if key == ("Unknown", "Unknown", "Unknown", "Unknown"):
            continue

        if key not in geo_dict:
            geo_dict[key] = geo_pk
            geo_pk += 1

    geo_rows = [[pk, *key] for key, pk in geo_dict.items()]
    
    save_csv(
        WAREHOUSE_DIR / "DimGeography.csv",
        ["geo_pk", "birth_place", "province", "region", "country"],
        geo_rows
    )

    # --- 2.2 DIM ARTIST ---
    # Generazione chiavi surrogate per Artisti e linking geografico.
    print("[INFO] Generating DimArtist...")
    
    artist_pk_lookup = {}
    artist_rows = []
    artist_pk = 1

    for a in artists:
        nat_id = a[a_idx[artist_id_col]]

        # Risoluzione FK Geografica
        loc_key = (
            a[a_idx["birth_place"]],
            a[a_idx["province"]],
            a[a_idx["region"]],
            a[a_idx["country"]]
        )
        # PK=0 per tutto Unkwnown
        if loc_key == ("Unknown", "Unknown", "Unknown", "Unknown"):
            geo_fk = 0
        else:
            geo_fk = geo_dict[loc_key]

        # Mappatura ID Originale -> Chiave Surrogata
        artist_pk_lookup[nat_id] = artist_pk

        artist_rows.append([
            artist_pk,
            nat_id,
            geo_fk,
            a[a_idx["name"]],
            a[a_idx["gender"]],
            a[a_idx["birth_date"]],
            a[a_idx["nationality"]],
            a[a_idx["description"]]
        ])
        artist_pk += 1

    save_csv(
        WAREHOUSE_DIR / "DimArtist.csv",
        [
            "artist_pk", "id_artist", "geo_fk", "name", "gender",
            "birth_date", "nationality", "description"
        ],
        artist_rows
    )

    # --- 2.3 DIM CATEGORY ---
    # Estrazione generi univoci dai brani.
    print("[INFO] Generating DimCategory...")
    
    category_dict = {}
    category_pk = 1

    for t in tracks:
        genre = t[t_idx["track_genre"]]
        if genre not in category_dict:
            category_dict[genre] = category_pk
            category_pk += 1

    category_rows = [[pk, genre] for genre, pk in category_dict.items()]
    
    save_csv(
        WAREHOUSE_DIR / "DimCategory.csv",
        ["category_pk", "category_name"],
        category_rows
    )

    # --- 2.4 DIM DATE ---
    # Parsing date e generazione key (YYYYMMDD). PK=0 per date nulle.
    print("[INFO] Generating DimDate...")
    
    date_rows = [[0, None, None, None, None, None, None]]
    seen_dates = set([0])

    for t in tracks:
        full_date = t[t_idx["full_date"]]

        if not full_date:
            continue

        # Generazione Key Integer
        pk = int(full_date.replace("-", ""))

        if pk in seen_dates:
            continue

        seen_dates.add(pk)

        date_rows.append([
            pk,
            full_date,
            t[t_idx["day"]],
            t[t_idx["month"]],
            t[t_idx["year"]],
            t[t_idx["release_weekday"]],
            t[t_idx["release_season"]]
        ])

    save_csv(
        WAREHOUSE_DIR / "DimDate.csv",
        ["date_song_pk", "full_date", "day", "month", "year", "weekday", "season"],
        date_rows
    )

    # --- 2.5 DIM AUDIO ---
    # Aggregazione tuple feature audio uniche.
    print("[INFO] Generating DimAudio...")
    
    audio_dict = {(None, None, None, None, None, None, None, None): 0}
    audio_pk = 1

    for t in tracks:
        key = (
            t[t_idx["bpm"]],
            t[t_idx["loudness"]],
            t[t_idx["flatness"]],
            t[t_idx["rolloff"]],
            t[t_idx["flux"]],
            t[t_idx["rms"]],
            t[t_idx["spectral_complexity"]],
            t[t_idx["pitch"]]
        )

        if key not in audio_dict:
            audio_dict[key] = audio_pk
            audio_pk += 1

    audio_rows = [[pk, *key] for key, pk in audio_dict.items()]
    audio_lookup = audio_dict # Riferimento per DimTrack

    save_csv(
        WAREHOUSE_DIR / "DimAudio.csv",
        [
            "audio_pk", "bpm", "loudness", "flatness", "rolloff",
            "flux", "rms", "spectral_complexity", "pitch"
        ],
        audio_rows
    )

    # --- 2.6 DIM LYRICS ---
    # Aggregazione tuple feature testuali uniche.
    print("[INFO] Generating DimLyrics...")
    
    lyrics_dict = {(None, None, None, None, None, None): 0}
    lyrics_pk = 1

    for t in tracks:
        key = (
            t[t_idx["n_tokens"]],
            t[t_idx["n_sentences"]],
            t[t_idx["avg_token_per_clause"]],
            t[t_idx["char_per_tok"]],
            t[t_idx["swear_IT"]],
            t[t_idx["swear_EN"]]
        )

        if key not in lyrics_dict:
            lyrics_dict[key] = lyrics_pk
            lyrics_pk += 1

    lyrics_rows = [[pk, *key] for key, pk in lyrics_dict.items()]
    lyrics_lookup = lyrics_dict # Riferimento per DimTrack

    save_csv(
        WAREHOUSE_DIR / "DimLyrics.csv",
        [
            "lyrics_pk", "n_tokens", "n_sentences",
            "avg_token_per_clause", "char_per_tok", "swear_IT", "swear_EN"
        ],
        lyrics_rows
    )

    # ==========================================================================
    # 3. GENERAZIONE TABELLE DI FACT E BRIDGE
    # ==========================================================================

    # --- 3.1 DIM TRACK ---
    # Costruzione tabella brani collegando Audio e Lyrics tramite FK.
    print("[INFO] Generating DimTrack...")
    
    track_pk_lookup = {}
    track_rows = []
    track_pk = 1

    for t in tracks:
        tid = t[t_idx["id"]]
        track_pk_lookup[tid] = track_pk

        # Risoluzione FK Audio
        audio_fk = audio_lookup[(
            t[t_idx["bpm"]], t[t_idx["loudness"]], t[t_idx["flatness"]],
            t[t_idx["rolloff"]], t[t_idx["flux"]], t[t_idx["rms"]],
            t[t_idx["spectral_complexity"]], t[t_idx["pitch"]]
        )]

        # Risoluzione FK Lyrics
        lyrics_fk = lyrics_lookup[(
            t[t_idx["n_tokens"]], t[t_idx["n_sentences"]],
            t[t_idx["avg_token_per_clause"]], t[t_idx["char_per_tok"]],
            t[t_idx["swear_IT"]], t[t_idx["swear_EN"]]
        )]

        track_rows.append([
            track_pk,
            tid,
            t[t_idx["title"]],
            t[t_idx["duration_ms"]],
            t[t_idx["explicit"]],
            t[t_idx["track_number"]],
            t[t_idx["disc_number"]],
            t[t_idx["original_source_id"]],
            t[t_idx["album_release_date"]],
            audio_fk,
            lyrics_fk
        ])
        track_pk += 1

    save_csv(
        WAREHOUSE_DIR / "DimTrack.csv",
        [
            "track_pk","id_track","title","duration_ms","explicit",
            "track_number","disc_number","original_source_id",
            "album_release_date","audio_fk","lyrics_fk"
        ],
        track_rows
    )

    # --- 3.2 BRIDGE FEATURED ---
    # relazione Many-to-Many tra Brani e Artisti.
    print("[INFO] Generating BridgeFeatured...")
    
    bridge_rows = []

    for t in tracks:
        track_fk = track_pk_lookup[t[t_idx["id"]]]

        # 1. Main Artist (Role = 1)
        main_nat_id = t[t_idx["id_artist"]]
        if main_nat_id in artist_pk_lookup:
             main_fk = artist_pk_lookup[main_nat_id]
             bridge_rows.append([track_fk, main_fk, 1])

        # 2. Featured Artists (Role = 0)
        # Parsiamo la lista separata da '|'
        feats = t[t_idx["featured_artists"]]
        if feats:
            for f in feats.split("|"):
                name_clean = f.strip().lower()
                # Risoluzione ID tramite nome (Lookup Name -> ID -> PK)
                if name_clean in name_to_id:
                    nat_id = name_to_id[name_clean]
                    if nat_id in artist_pk_lookup:
                        bridge_rows.append([track_fk, artist_pk_lookup[nat_id], 0])

    save_csv(
        WAREHOUSE_DIR / "BridgeFeatured.csv",
        ["track_fk","artist_fk","role"],
        bridge_rows
    )

    # --- 3.3 FACT SONG STREAMS ---
    # Tabella dei fatti transazionale.
    print("[INFO] Generating FactSongStreams...")
    
    fact_rows = []

    for t in tracks:
        track_fk = track_pk_lookup[t[t_idx["id"]]]
        category_fk = category_dict[t[t_idx["track_genre"]]]

        fd = t[t_idx["full_date"]]
        date_fk = int(fd.replace("-", "")) if fd else 0

        fact_rows.append([
            date_fk,
            track_fk,
            category_fk,
            t[t_idx["streams@1month"]],
            t[t_idx["popularity"]]
        ])

    save_csv(
        WAREHOUSE_DIR / "FactSongStreams.csv",
        [
            "date_song_fk","track_fk","category_fk",
            "streams_1month","popularity"
        ],
        fact_rows
    )

    print("=== PIPELINE DONE: FILES READY FOR SSIS ===")

if __name__ == "__main__":
    main()
"""
GRUPPO 13 - Assignment 3: Song Profiling
Descrizione: Arricchimento semantico del dataset tramite tecniche di Clustering.

"""

import json
import math
import random
from pathlib import Path

# ==============================================================================
# CONFIGURAZIONE AMBIENTE E PERCORSI
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
CLEANED_DIR = BASE_DIR / "00_data" / "cleaned"
ENRICHED_DIR = BASE_DIR / "00_data" / "enriched"

# Assicura l'esistenza della directory di output
ENRICHED_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# FUNZIONI DI ESTRAZIONE FEATURE
# ==============================================================================

def get_safe_float(value, default=0.0):
    """
    Conversione sicura a float. Gestisce None e errori di casting.
    Fondamentale per prevenire crash durante i calcoli matematici.
    """
    if value is None:
        return default
    try:
        return float(value)
    except:
        return default

def extract_features(track):
    """
    Estrae il vettore delle feature numeriche (12 dimensioni) per ogni brano.
    Combina metriche audio (es. BPM, Loudness) e metriche testuali (es. n_tokens, swear_words).
    """
    # Recupero conteggi parole proibite (gestione null -> 0)
    swear_it = track.get("swear_IT") or 0
    swear_en = track.get("swear_EN") or 0
    
    # Feature ingegnerizzata: Intensità totale contenuto esplicito
    total_swear = float(swear_it + swear_en)

    # Costruzione vettore feature
    features = [
        get_safe_float(track.get("bpm"), 0.0),
        get_safe_float(track.get("loudness"), -20.0),
        get_safe_float(track.get("flatness"), 0.0),
        get_safe_float(track.get("flux"), 0.0),
        get_safe_float(track.get("spectral_complexity"), 0.0),
        get_safe_float(track.get("pitch"), 0.0),
        get_safe_float(track.get("rms"), 0.0),
        get_safe_float(track.get("rolloff"), 0.0),
        get_safe_float(track.get("n_tokens"), 0),
        total_swear,  # Frequenza parole offensive
        get_safe_float(track.get("explicit"), 0)
    ]
    return features


# ==============================================================================
# IMPLEMENTAZIONE ALGORITMI (K-MEANS E Z-SCORE)
# Nota: Implementiamo K-Means e Z-Score manualmente senza librerie ML esterne.
# ==============================================================================

def standardize_data(dataset):
    """
    Applica la Z-Score Normalization al dataset.
    Formula: z = (x - mean) / std_dev
    
    Necessaria perché le feature hanno scale molto diverse (es. BPM ~120 vs Flatness ~0.1).
    Senza normalizzazione, la distanza Euclidea sarebbe dominata dalle feature con magnitudine maggiore.
    """
    if not dataset: return []
    
    n_rows = len(dataset)
    n_cols = len(dataset[0])

    # 1. Calcolo delle Medie per colonna
    means = [0.0] * n_cols
    for col in range(n_cols):
        col_sum = sum(row[col] for row in dataset)
        means[col] = col_sum / n_rows

    # 2. Calcolo della Deviazione Standard per colonna
    stds = [0.0] * n_cols
    for col in range(n_cols):
        variance_sum = sum((row[col] - means[col])**2 for row in dataset)
        # Evitiamo divisione per zero se la feature è costante
        stds[col] = math.sqrt(variance_sum / n_rows)
        if stds[col] == 0: stds[col] = 1.0

    # 3. Trasformazione dei dati
    normalized_data = []
    for row in dataset:
        new_row = []
        for col in range(n_cols):
            z_val = (row[col] - means[col]) / stds[col]
            new_row.append(z_val)
        normalized_data.append(new_row)

    return normalized_data


def kmeans_clustering(dataset, k, max_iterations=40):
    """
    Implementazione manuale dell'algoritmo K-Means.
    
    Fasi:
    1. Inizializzazione: Scelta casuale di k centroidi.
    2. Assegnazione: Ogni punto è assegnato al centroide più vicino (Distanza Euclidea).
    3. Aggiornamento: Ricalcolo dei centroidi come media dei punti nel cluster.
    4. Iterazione: Ripetizione fino a convergenza o max iterazioni.
    """
    n_features = len(dataset[0])
    
    # Step 1: Inizializzazione casuale
    centroids = random.sample(dataset, k)
    labels = [0] * len(dataset)

    for iteration in range(max_iterations):
        # Preparazione contenitori cluster vuoti
        clusters = [[] for _ in range(k)]
        new_labels = []

        # Step 2: Assegnazione punti ai cluster
        for point in dataset:
            # Calcolo distanza euclidea da tutti i centroidi
            distances = []
            for center in centroids:
                dist = sum((point[i] - center[i])**2 for i in range(n_features))
                distances.append(dist)
            
            # Assegnazione all'indice con distanza minima
            best_cluster = distances.index(min(distances))
            clusters[best_cluster].append(point)
            new_labels.append(best_cluster)
        
        # Verifica convergenza (se le etichette non cambiano, stop)
        if new_labels == labels:
            break
        labels = new_labels

        # Step 3: Ricalcolo posizioni centroidi (Media geometrica)
        new_centroids = []
        for i in range(k):
            points_in_cluster = clusters[i]
            if not points_in_cluster:
                # Gestione cluster vuoto (Edge Case): riassegna centroide casuale
                new_centroids.append(random.choice(dataset))
            else:
                # Calcolo media per ogni dimensione
                avg_point = []
                for feat_idx in range(n_features):
                    feat_sum = sum(p[feat_idx] for p in points_in_cluster)
                    avg_point.append(feat_sum / len(points_in_cluster))
                new_centroids.append(avg_point)
        
        centroids = new_centroids

    return labels, centroids


# ==============================================================================
# PIPELINE DI ESECUZIONE (MAIN)
# ==============================================================================

def main():
    # Fix del seed randomico per garantire riproducibilità dei risultati (cluster stabili)
    random.seed(42) 

    input_file = CLEANED_DIR / "tracks_cleaned.json"
    output_file = ENRICHED_DIR / "tracks_A3.json"

    print(f"[INFO] Loading data from {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        tracks = json.load(f)

    # 1. Feature Extraction (Preprocessing)
    print("[INFO] Extracting features...")
    raw_features = [extract_features(t) for t in tracks]

    # 2. Normalizzazione Dati (Z-Score)
    print("[INFO] Standardizing data (Z-Score)...")
    normalized_features = standardize_data(raw_features)

    # 3. Esecuzione Clustering (K-Means)
    k = 6
    print(f"[INFO] Running K-Means with k={k}...")
    labels, centers = kmeans_clustering(normalized_features, k)

    # 4. Assegnazione Etichette di Genere (Semantic Mapping)
    # Mapping derivato dall'analisi a posteriori delle caratteristiche dei centroidi.
    cluster_mapping = {
        0: "SOFT POP",  # Bassa energia, pochi testi espliciti
        1: "MINIMAL",   # Valori negativi su molte feature (sotto la media)
        2: "RAP",       # Alto contenuto di parole (tokens) e swear words
        3: "POP",       # Valori medi, alta loudness
        4: "URBAN",     # Cluster outlier con valori molto bassi
        5: "DANCE"      # Alto BPM e caratteristiche ritmiche
    }

    for i, track in enumerate(tracks):
        cluster_id = labels[i]
        genre_name = cluster_mapping.get(cluster_id, "UNKNOWN")
        track["track_genre"] = genre_name

    # 5. Stampa Statistiche e Analisi Centroidi (Profiling Output)
    print("\n=== CLUSTERING RESULTS (Counts) ===")
    counts = {}
    for t in tracks:
        g = t["track_genre"]
        counts[g] = counts.get(g, 0) + 1
    
    for genre, count in counts.items():
        print(f"{genre:10s} : {count} tracks")

    print("\n=== CENTROIDS ANALYSIS ===")
    feature_names = [
        "bpm", "loudness", "flatness", "flux", "complexity",
        "pitch", "rms", "rolloff", "tokens", "swear_count", "explicit"
    ]
    
    for i, center in enumerate(centers):
        print(f"\n--- Cluster {i} ({cluster_mapping.get(i)}) ---")
        for name, val in zip(feature_names, center):
            print(f"{name:15s}: {val:.3f}")

    # 6. Salvataggio Dataset Arricchito
    print(f"\n[INFO] Saving enriched dataset to {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tracks, f, ensure_ascii=False, indent=2)

    print("[INFO] Song profiling completed successfully.")

if __name__ == "__main__":
    main()


'''
=== OUTPUT DI RIFERIMENTO ===
=== CLUSTERING RESULTS (Counts) ===
RAP        : 1093 tracks
POP        : 1897 tracks
SOFT POP   : 3094 tracks
DANCE      : 2971 tracks
MINIMAL    : 2037 tracks
URBAN      : 68 tracks

=== CENTROIDS ANALYSIS ===

--- Cluster 0 (SOFT POP) ---
bpm            : 0.107
loudness       : 0.481
flatness       : 0.234
flux           : 0.293
complexity     : 0.033
pitch          : 0.382
rms            : 0.478
rolloff        : -0.276
tokens         : 0.120
swear_count    : -0.064
explicit       : 0.990

--- Cluster 1 (MINIMAL) ---
bpm            : -0.006
loudness       : -1.270
flatness       : -0.113
flux           : -0.305
complexity     : -1.112
pitch          : -0.129
rms            : -1.366
rolloff        : -0.094
tokens         : -0.411
swear_count    : -0.256
explicit       : -0.097

--- Cluster 2 (RAP) ---
bpm            : -0.027
loudness       : 0.150
flatness       : 0.007
flux           : 0.615
complexity     : -0.033
pitch          : 0.428
rms            : 0.136
rolloff        : 0.182
tokens         : 1.298
swear_count    : 2.691
explicit       : 0.413

--- Cluster 3 (POP) ---
bpm            : -0.113
loudness       : 0.306
flatness       : -0.498
flux           : 0.017
complexity     : 1.111
pitch          : -0.617
rms            : 0.344
rolloff        : 1.196
tokens         : 0.068
swear_count    : -0.050
explicit       : -0.096

--- Cluster 4 (URBAN) ---
bpm            : -4.090
loudness       : -4.689
flatness       : -6.507
flux           : -7.516
complexity     : -3.144
pitch          : -5.374
rms            : -3.339
rolloff        : -2.777
tokens         : -0.206
swear_count    : -0.201
explicit       : -0.190

--- Cluster 5 (DANCE) ---
bpm            : 0.059
loudness       : 0.289
flatness       : 0.300
flux           : -0.074
complexity     : 0.130
pitch          : 0.113
rms            : 0.311
rolloff        : -0.401
tokens         : -0.160
swear_count    : -0.319
explicit       : -1.009
'''
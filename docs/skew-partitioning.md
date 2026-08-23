# Bilanciamento del carico: skew delle partizioni Kafka e dei subtask Flink

Nota di lavoro. Analisi svolta sul dataset completo (100 giorni, 2026-01-02 → 2026-05-22,
61 asset, ~60M tick), conteggi per simbolo/giorno estratti da
`ScriptMonte/Tesi/Data/*.csv`.

Metrica usata ovunque: **max/mean** = carico del contenitore più pieno diviso il carico
medio. È la metrica giusta perché il tempo di uno stadio è dettato dall'ultimo che finisce,
quindi `max/mean = 2` significa "ci metti il doppio del necessario". `max/min` è fuorviante
(un contenitore vuoto non rallenta nessuno) e va usata solo come colore.

Metodologia: calibrazione **solo sui primi 40 giorni** (warmup), misura sui 60 successivi.
Nessun uso di dati futuri, coerente con il walk-forward dei modelli.

---

## 1. Il problema

Sia Kafka sia Flink assegnano i simboli con un hash **cieco al carico**: distribuiscono
uniformemente il *numero di simboli*, non il *numero di tick*. Con 61 asset e un rapporto
max/min di 1850x tra il simbolo più attivo (GOOGL, ~70k tick/giorno) e il meno attivo
(BBD, ~40), l'esito è pesantemente sbilanciato.

Sono **due hash diversi in due punti diversi**: il `keyBy(symbol)` in `FlinkJob.java:226`
rimescola tutto da capo dopo Kafka, quindi sistemarne uno non sistema l'altro.

| stadio | assegnazione | max/mean (test, 60gg) |
|---|---|---|
| Kafka, 8 partizioni | `murmur2(symbol) % 8` | **2,37x** (misurato poi: 2,325x) |
| Flink, 12 subtask | `murmurHash(hashCode) % 128` → key group → subtask | **2,62x** (mai verificato) |

Valori da simulazione sui conteggi. Quello Kafka e' stato poi confermato dagli offset reali
(§4); quello Flink descrive una distribuzione di record che, come mostra la misura, non
determina il tempo di esecuzione.

Esempio concreto (giorno 2026-01-02, Flink p=12, mp=128):

```
thread 11: 147.593 tick      thread 10: 9.020 tick
```

Il thread 11 fa 16 volte il lavoro del thread 10. Gli altri undici finiscono e aspettano.
È questo che spiega il plateau in `benchmark.csv`: da p=6 a p=12 il throughput non sale
(1.766K → 1.734K tick/s) perché i thread aggiunti restano quasi inattivi.

### Limite invalicabile

Tutti i tick di un simbolo vanno **sempre** allo stesso subtask, perché lì vive il suo stato
(accumulatori delle finestre, buffer 22 giorni). La chiave è indivisibile. Quindi:

```
P* = tick_totali / tick_del_simbolo_più_grosso
```

Sul dataset: **P* mediana = 9** (min 2, max 14). Solo 3 giorni su 100 saprebbero usare più
di 12 subtask. Oltre P* nessuna assegnazione può bilanciare — servirebbe spezzare le chiavi
(partitioning policy di Alps, cfr. §5).

---

## 2. Flink `maxParallelism` — tentativo, MISURATO INEFFICACE

Riportato perche' e' un risultato negativo utile, non perche' vada applicato.

### L'ipotesi e da dove usciva 1096

`maxParallelism` fissa il numero di key group, cioe' la mappa symbol -> subtask
(`murmurHash(key.hashCode()) % maxParallelism` -> key group -> subtask). Cambiandolo cambia
quali simboli finiscono insieme. **Ricerca a forza bruta**, non una formula:

```python
best = min(range(12, 3000),
           key=lambda mp: sbilanciamento_medio(mp, primi_40_giorni))
# -> 1096
```

Non c'e' continuita': `mp=1096` da' 1,58x e `mp=1097` da' 2,12x. In simulazione lo
sbilanciamento dei subtask scendeva da 2,62x a 1,63x (da ~4,6 a ~7,4 thread utili su 12),
validato fuori campione sui 60 giorni di test.

**Spiegazione sbagliata da non usare:** *non* e' vero che "piu' key group = grana piu' fine =
carico mediato". La media di max/mean e' ~2,6x in ogni fascia di valori (120-200, 500-600,
1000-1100, 2000-2100) e i simboli per subtask restano 3-9 in entrambi i casi. Alzare
`maxParallelism` non aiuta di per se': 1096 e' solo la permutazione che, per *questi 61 nomi*,
separa GOOGL, GOOG, AMZN, AAPL e AMD.

### Cosa dice la misura

| topic | `mp=128` | `mp=1096` | delta |
|---|---|---|---|
| sbilanciato (`hash`) | 26.819 ms | 26.505 ms | -1,2% |
| bilanciato (`lpt`) | 19.854 ms | 19.770 ms | -0,4% |

**Nessun effetto**, in entrambi i casi dentro il rumore di misura. Una prima tornata a singola
ripetizione aveva mostrato -5,6%: era un artefatto, sparito con 3 ripetizioni.

### Perche' la simulazione prevedeva un guadagno che non esiste

La simulazione misurava correttamente la **distribuzione dei record** fra i subtask. Ma quella
distribuzione non e' il collo di bottiglia: gli operatori finestra hanno accumulatori a stato
costante (`LogRetAcc` 40 byte, `RvAcc` 12 byte) e costano pochissimo per tick. Il tempo e'
dettato dallo stadio source (deserializzazione Avro), che dipende dalle partizioni Kafka, non
dai key group.

**Lezione metodologica, da mettere in tesi:** una metrica di bilanciamento del carico predice
la distribuzione del lavoro, non il throughput. Vale solo se lo stadio sbilanciato e' anche
quello che domina il tempo. Qui non lo era.

## 3. Fix Kafka — mappa LPT ricalcolata ogni sera

A differenza di Flink, qui la mappa vive nel producer: cambiarla costa zero (nessuno stato da
migrare) e al confine di giornata le finestre hanno già emesso. Quindi può essere **dinamica**.

### Algoritmo

```
ogni sera, a mercato chiuso:

    previsto[asset] = media dei tick degli ultimi 2 giorni

    carico = [0] * 8
    per asset in ordine di previsto DECRESCENTE:
        i = indice del minimo di carico
        mappa[asset] = i
        carico[i] += previsto[asset]

    usa `mappa` per il giorno successivo
```

È LPT (Longest Processing Time first): piazzi prima i massi, poi la ghiaia riempie i buchi.
`carico` è un contatore interno che parte da zero ogni sera, **non** il carico osservato ieri.

### Perché la media a 2 giorni

I conteggi dei tick fanno **clustering come la volatilità**: ieri predice oggi molto meglio di
una media lunga. Le medie lunghe inseguono con ritardo i cambi di regime, e LPT — che impacca
le partizioni per essere esattamente uguali sui volumi *previsti* — amplifica ogni errore di
stima: basta un asset sottostimato e quella partizione esplode.

| predittore | media | p95 | peggiore |
|---|---|---|---|
| hash murmur2 (baseline) | 2,37x | 2,67x | 3,18x |
| media ultimi 1gg | 1,32x | 2,33x | 3,18x |
| **media ultimi 2gg** | **1,28x** | **2,25x** | **3,18x** |
| media ultimi 3gg | 1,30x | 2,33x | 4,69x |
| media ultimi 5gg | 1,35x | 3,75x | 4,34x |
| media ultimi 20gg | 1,41x | 4,08x | 5,27x |
| media espandente (tutto lo storico) | 1,51x | 5,94x | **7,27x** |
| oracolo (vede il futuro) | 1,09x | 2,25x | 3,18x |

La finestra a 2 giorni eguaglia l'oracolo su p95 e peggior caso. La media espandente è la
scelta peggiore: in coda arriva a 7,27x, **peggio del non fare niente**.

### Raffinare l'impacchettamento non serve

| | su volumi previsti | nella realtà |
|---|---|---|
| LPT greedy | 1,029x | 1,28x |
| LPT + local search | 1,026x | 1,28x |

LPT greedy bilancia già quasi perfettamente i volumi previsti. Tutto il divario 1,03 → 1,28 è
**errore di previsione**, non di impacchettamento. Lo spazio residuo (1,28 → 1,09) si prende
solo con un predittore migliore dei tick giornalieri — che sarebbe un modello tipo HAR sui
conteggi. Fuori scope.

### Implementazione

1. Pass di profiling che conta i tick per simbolo/giorno (o riuso dei conteggi del notebook).
2. Calcolo LPT → mappa di 61 voci, serializzata su file.
3. `Partitioner` custom in `Executor.java` (oggi la key `symbol` attiva il `DefaultPartitioner`
   a riga 86): a runtime è un lookup in HashMap, costo nullo.

### Nota su ordinamento e willingness

Ricalcolare ogni sera sposta in media **24,5 simboli su 61**. Ogni spostamento è una
discontinuità nella garanzia d'ordine per-key (i tick di ieri sono nella partizione 3, quelli
di oggi nella 5). Qui — e solo qui — avrebbe senso una *willingness function* stile Alps:
decidere se il guadagno di bilanciamento vale il numero di chiavi rilocate. Opzionale: senza,
si ricalcola sempre e funziona lo stesso.

---

## 4. Risultati misurati

Benchmark end-to-end sul dataset completo (100 giorni, 66.820.195 tick), 8 partizioni Kafka,
`parallelism=12`, `ml.enabled=false` per isolare Flink dal server ML. 3 ripetizioni per
configurazione; si riportano le mediane perche' le distribuzioni hanno code lunghe.

### Bilanciamento delle partizioni Kafka (misurato dagli offset, non simulato)

```
        hash                       lpt
part 3: 19.417.406          part 4:  8.526.372
part 7: 12.068.473          part 2:  8.445.027
part 0:  7.460.591          part 1:  8.400.351
part 4:  6.779.534          part 3:  8.370.854
part 2:  6.285.785          part 7:  8.339.311
part 5:  6.128.522          part 6:  8.311.514
part 1:  4.474.793          part 5:  8.308.995
part 6:  4.205.091          part 0:  8.117.771
  max/mean = 2,325x           max/mean = 1,021x
```

Nota: questo e' lo sbilanciamento **cumulato sui 100 giorni**, ed e' migliore del 1,28x
previsto per giorno perche' gli errori di previsione dei singoli giorni si compensano. Sono
due metriche valide per due domande diverse: 1,02x descrive il replay bulk, 1,28x descrive
uno scenario streaming giorno per giorno.

### Tempo di esecuzione Flink

| config | misure (ms) | mediana | throughput |
|---|---|---|---|
| `hash` + `mp=128` (stato di partenza) | 26029, 26819, 31188 | 26.819 | 2,49M tick/s |
| `hash` + `mp=1096` | 26296, 26505, 29220 | 26.505 | 2,52M tick/s |
| **`lpt` + `mp=128`** | 19060, 19854, 23872 | **19.854** | **3,37M tick/s** |
| `lpt` + `mp=1096` | 19511, 19770, 27344 | 19.770 | 3,38M tick/s |

**Effetto del partitioner** (aggregando i due `mp`, n=6 per gruppo): mediana 26.662 -> 19.812 ms,
**-25,7%**. In 32 coppie su 36 una misura `lpt` batte una `hash`; test di permutazione esatto
**p = 0,013**. Le distribuzioni si sovrappongono solo per i due valori anomali di `lpt`.

**Effetto di `maxParallelism`**: -1,2% e -0,4%, dentro il rumore. Nessun effetto.

### Avvertenza sulla misura

La dispersione fra ripetizioni identiche e' alta (fino al 25-38%) e sembra bimodale
(~19,5s contro ~24-27s), quindi ha una causa discreta piuttosto che rumore continuo —
plausibilmente il checkpointing o la page cache del broker. Non e' stata indagata. Per i
numeri definitivi della tesi conviene aumentare le ripetizioni e/o identificare la causa,
perche' con questa varianza qualunque effetto sotto il ~10% non e' misurabile su questa
macchina.

---

## 4bis. Riepilogo operativo

| stadio | intervento | esito |
|---|---|---|
| Kafka (8 partizioni) | mappa LPT su media 2gg, ricalcolata ogni sera | **2,325x -> 1,021x, throughput +35%** |
| Flink (12 subtask) | `setMaxParallelism(1096)` | **nessun effetto misurabile** |

Il fix Kafka fa tutto il lavoro. Il fix Flink va rimosso o dichiarato come risultato negativo:
non e' giustificabile tenere in produzione una costante tarata sui nomi degli asset, congelata
al primo checkpoint, che non produce alcun guadagno.

Limite da dichiarare in tesi: *l'adattivita' e' confinata allo stadio di ingestione; il livello
di partizionamento interno di Flink resta statico per vincolo del modello di key group — e la
misura mostra che li' non c'e' comunque niente da guadagnare, perche' il collo di bottiglia e'
lo stadio source.*

## 5. Cosa è stato scartato, e perché

**Numero di partizioni / parallelismo dinamici.** Sei già al soffitto: P* mediana = 9, e giri
con 8 partizioni. Renderlo dinamico varrebbe ~12% contro il +58% dello skew fix. Inoltre Kafka
**non permette di ridurre** le partizioni (solo aumentarle), quindi "scendere nei giorni
scarichi" non è implementabile; e il parallelismo dinamico in Flink richiede l'Adaptive
Scheduler con savepoint + restart a ogni riscalatura.

**Willingness function completa (Alps, DEXA 2022).** Zou et al., *Alps: An Adaptive Load
Partitioning Scaling Solution for Stream Processing System on Skewed Stream*, LNCS 13426,
pp. 17-31, DOI 10.1007/978-3-031-12426-6_2. La willingness function del paper arbitra tra
**scaling policy** e **partitioning policy**. Nella pipeline non esiste un attuatore di
scaling (parallelismo fisso alla submit, job bounded in replay), quindi non ha nulla da
arbitrare. Va citato come related work spiegando questa differenza.

**Key splitting.** È l'unico modo di superare P* ≈ 9: far calcolare a più subtask risultati
parziali dei simboli caldi e ricombinarli. Fattibile in linea di principio (la RV è una somma,
e l'accumulatore della finestra 5-min si può rendere combinabile portandolo a
`(firstTs, firstMid, lastTs, lastMid)`), ma è una riscrittura degli aggregatori. Sviluppo
futuro, non intervento attuale.

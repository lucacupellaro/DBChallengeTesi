# Garanzia di consegna: da at-least-once a effectively-once

Nota di lavoro. Riguarda una discrepanza fra quanto la scaletta dichiara (§2.6, §5.5) e
quanto il codice garantiva.

## Il problema

```java
FlinkJob.java   env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
FlinkJob.java   connection.setAutoCommit(true);
```

`CheckpointingMode.EXACTLY_ONCE` non e' una garanzia di consegna: riguarda lo **stato**. Dice
che dopo un riavvio da checkpoint ogni record ha contribuito agli accumulatori esattamente una
volta (la somma dei quadrati per la RV, il buffer 22 giorni). Lo ottiene con il *barrier
alignment*: un operatore con piu' ingressi aspetta il barrier da tutti prima di fotografare il
proprio stato.

La garanzia end-to-end richiede tre pezzi, e vale il piu' debole:

| | | prima |
|---|---|---|
| 1 | sorgente rigiocabile (Kafka, offset nel checkpoint) | ✓ |
| 2 | stato coerente (checkpoint EXACTLY_ONCE) | ✓ |
| 3 | sink che non lascia doppioni nel mondo esterno | ✗ |

Il sink e' una `SinkFunction` custom con `autoCommit=true`: ogni INSERT e' committato subito,
fuori dal protocollo dei checkpoint, e Flink non puo' annullarlo.

Cosa succedeva davvero:

```
t=0    checkpoint salvato, offset Kafka = 1.000.000
t=0-15 Flink legge fino a 1.400.000 e scrive 400 predizioni in Postgres
t=15   crash
t=16   restart dall'offset 1.000.000 -> rilegge, ricalcola, RISCRIVE le stesse 400
```

Stato perfetto, ma 400 righe duplicate in `predictions`: **at-least-once end-to-end**.

## La soluzione: sink idempotente

Idempotente = ripetere l'operazione non cambia il risultato, `f(f(x)) = f(x)`. L'INSERT
originale non lo era: creava una riga nuova a ogni esecuzione (come `x += 1`), perche' `id` e'
`BIGSERIAL`.

`(symbol, ts)` e' la chiave logica: la topologia emette **una sola predizione per simbolo per
giorno** (finestra giornaliera con `keyBy(symbol)`), quindi quella coppia identifica il record.
`id` e `created_at` sono metadati tecnici e al replay cambiano per costruzione — si deduplica
sulla chiave logica, non sulla riga intera.

```sql
-- init.sql
ALTER TABLE predictions ADD CONSTRAINT predictions_symbol_ts_key UNIQUE (symbol, ts);
```
```sql
-- FlinkJob, INSERT_SQL
INSERT INTO predictions (...) VALUES (...) ON CONFLICT (symbol, ts) DO NOTHING
```

**Il vincolo UNIQUE da solo non basta e anzi peggiora le cose**: senza `ON CONFLICT` l'insert
duplicato solleva una `SQLException`, il task fallisce, Flink riparte da checkpoint e ritenta
lo stesso insert — loop di restart. Il vincolo definisce l'identita', `ON CONFLICT` dice cosa
fare quando si presenta.

La scrittura passa da *"aggiungi una riga"* a *"fai in modo che la riga esista"*. Il replay
produce le stesse chiavi, che vengono assorbite.

### `DO NOTHING` e non `DO UPDATE`

Il server ML tiene `symbol_history` al proprio interno, **fuori dal checkpoint di Flink**. Al
riavvio Flink torna indietro, il server ML no: la sua storia per simbolo e' gia' avanti, quindi
la predizione ricalcolata puo' differire numericamente dall'originale. `DO NOTHING` conserva la
prima, prodotta con lo stato ML coerente con quel punto dello stream.

## Verifica

Topic con 30 giorni, due replay integrali consecutivi (`ml.enabled=false`):

```
run 1:  332 righe,  332 distinte
run 2:  332 righe,  332 distinte
```

Prima della modifica la seconda run avrebbe prodotto 664 righe.

## Come dichiararlo in tesi

Non "exactly-once" generico. La formulazione precisa:

> Il sistema garantisce *effectively-once* sul database: la sorgente Kafka e' rigiocabile, lo
> stato degli operatori e' consistente per checkpoint, e il sink e' idempotente sulla chiave
> logica `(symbol, ts)`. Non si tratta di *exactly-once delivery* — il sink non partecipa a un
> commit a due fasi — ma il risultato osservabile e' equivalente: ogni predizione compare una
> sola volta.

E il limite, da dichiarare esplicitamente:

> Il server di inferenza mantiene stato proprio (`symbol_history`) non gestito dal checkpoint
> di Flink. L'idempotenza del sink lo rende innocuo per il risultato persistito, ma la garanzia
> vale sul database, non sull'inferenza: dopo un riavvio alcune predizioni vengono ricalcolate
> su uno stato del modello disallineato, e sono quelle che `DO NOTHING` scarta.

Chiudere anche quest'ultimo punto richiederebbe di mettere sotto checkpoint lo stato del server
ML, oppure di renderlo stateless spostando la storia per simbolo dentro Flink (che gia' la
possiede nel buffer 22 giorni e la invia alla prima chiamata). La seconda strada e' realistica
ed e' un possibile sviluppo futuro.

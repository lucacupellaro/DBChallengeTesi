# DBChallengeTesi

## Prerequisiti

- Java 8+
- Maven
- Docker e Docker Compose
- Python 3.12+ con `mamba-ssm`, `torch`, `fastapi`, `uvicorn`

## Come lanciare

### 1. Avvia l'infrastruttura (Kafka, Zookeeper, Schema Registry, PostgreSQL)

```bash
docker compose up -d
```

Aspetta ~15 secondi che tutti i container siano pronti:

```bash
docker compose ps
```

### 2. Compila il progetto

```bash
mvn package -q -DskipTests
```

### 3. Carica i tick su Kafka (Producer)

```bash
java -jar target/Tesi-1.0-SNAPSHOT.jar producer src/main/java/File/Dati
```

### 4. Avvia il server ML (in un altro terminale)

```bash
/home/lucacupellaro/luca/Validator/.venv/bin/python src/main/ml-server/server.py
```

### 5. Lancia il Flink job (in un altro terminale)

```bash
java -jar target/Tesi-1.0-SNAPSHOT.jar consumer
```

### 6. Verifica le predizioni nel DB

```bash
docker exec postgres psql -U flink -d volatility -c "SELECT * FROM predictions LIMIT 10;"
docker exec postgres psql -U flink -d volatility -c "SELECT symbol, COUNT(*) FROM predictions GROUP BY symbol ORDER BY count DESC;"
docker exec postgres psql -U flink -d volatility -c "SELECT COUNT(*) FROM predictions WHERE mamba_pred IS NOT NULL;"
```

## Connessione PostgreSQL

```
Host: localhost
Port: 5433
Database: volatility
User: flink
Password: flink
```

## Shutdown

```bash
docker compose down
```

Per cancellare anche i volumi (dati Kafka e PostgreSQL):

```bash
docker compose down -v
```

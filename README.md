# Currency exchange ETL (Airflow + MongoDB)

A daily Airflow pipeline, built with the Astro CLI, that reads EUR exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them and stores them in MongoDB. Mongo Express is the
web admin UI for the database.

```
extract_exchange_rates  >>  transform_exchange_rates  >>  load_to_mongodb
```

| Task | What it does | Named XCom |
|---|---|---|
| `extract_exchange_rates` | calls `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY` | pushes `raw_rates` |
| `transform_exchange_rates` | builds one document per currency | pulls `raw_rates`, pushes `transformed_rates` |
| `load_to_mongodb` | upserts the documents into `currency_db.exchange_rates` | pulls `transformed_rates` |

Stored document:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1123,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T07:15:00"
}
```

The load is idempotent: a unique index on (`base_currency`, `target_currency`, `rate_date`) plus an upsert means
running the DAG twice on the same day updates the documents instead of duplicating them.

## Docker networking and persistent storage

`docker-compose.override.yml` adds two services to the Astro stack:

* `mongo` (MongoDB 7) and `mongo-express` join the network `airflow`, so Airflow reaches the database by its
  service name (`mongo:27017`). MongoDB itself publishes no port; only Mongo Express is published, on
  `127.0.0.1:8081`.
* MongoDB stores its files in the named volume `mongo_data` (`/data/db`), so the data survives
  `astro dev stop` / `astro dev start`. Only `astro dev kill` deletes it.

Both are also written as `LABEL`s on the project image (`Dockerfile`) and as `labels` on the services.

## Run it

1. Install Docker Desktop and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
2. Copy `.env.example` to `.env` and choose a password (`.env` is git-ignored).
3. Start everything:
   ```bash
   astro dev start
   ```
4. Open Airflow at http://127.0.0.1:8080, unpause `currency_exchange_etl` and trigger it.
5. Open Mongo Express at http://127.0.0.1:8081 and look at `currency_db` > `exchange_rates`.
6. Run the tests: `astro dev pytest`.

Stop with `astro dev stop` (keeps the data) or `astro dev kill` (removes containers and volumes).

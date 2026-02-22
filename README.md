# Financial Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8+-017CEE.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B.svg)](https://www.getdbt.com/)

Playing around with market data + sentiment analysis. Mostly an excuse to learn Great Expectations and practice data quality stuff.

## The idea

What if we could correlate Reddit sentiment with stock movements? Probably nothing groundbreaking but it's fun to check. This pipeline grabs:
- Market data from Yahoo Finance
- News headlines
- Reddit comments from r/wallstreetbets (for entertainment value)

Then tries to make sense of it all.

## Current state

Honestly? It's a work in progress. The Airflow DAGs run, data lands in BigQuery, dbt models exist. But the sentiment analysis part is... basic. Like, really basic. spaCy + some keyword matching. Nothing fancy.

## What's actually done

- Airflow DAGs for daily data ingestion
- Basic data quality checks with Great Expectations
- dbt models for cleaning and transforming
- Some experiments with correlation analysis (Jupyter notebooks)

## What's broken / TODO

- [ ] The Reddit API rate limits are annoying
- [ ] Need better sentiment scoring (current one is too naive)
- [ ] Data lineage tracking - started but not complete
- [ ] Actually prove if sentiment predicts anything (spoiler: probably not)
- [ ] Add proper alerting when data quality checks fail

## Tech stack

- **Airflow** for orchestration
- **BigQuery** for warehouse
- **dbt** for transformation
- **Great Expectations** for data validation
- **spaCy** for NLP (I know, should probably use transformers but ¯\_(ツ)_/¯)

## Running it

```bash
git clone https://github.com/CR7REX/financial-data-platform.git
cd financial-data-platform
docker-compose up -d
```

You'll need API keys for Reddit and NewsAPI if you want the full thing.

## Lessons learned

- Free financial APIs have... quirks. Yahoo Finance isn't officially an API, just a hack that could break any day.
- Data quality matters more than you think. One bad timestamp ruins everything.
- r/wallstreetbets sentiment is basically noise. Who knew?
- Great Expectations is powerful but the learning curve is real

## Disclaimer

This is for learning purposes only. Don't trade based on this. Seriously. I'm not responsible if you lose money.

---

*Made for educational purposes. The only thing this predicts is that I'll spend too much time debugging Airflow.*

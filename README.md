#GraphRec — Graph-Based Recommendation System with UI


---

## Tech Stack
- Apache Spark 3.4.1 (PySpark DataFrames)
- Flask (Python web server + REST API)
- Full Web UI (HTML/CSS/JS — no framework)
- Docker + Docker Compose

## UI Pages
| Page | What it shows |
|------|---------------|
| Dashboard | Graph stats + category PageRank bar chart |
| Recommend | Select user → see top-N recommendations + history |
| PageRank | Top 20 products by PageRank score |
| Categories | Avg/max PageRank + product count per category |
| Products | Full catalogue with search + filter |
| About | Algorithm pipeline explanation |

## Run with Docker (recommended)

```bash
# 1. Generate dataset
cd data && python generate_data.py && cd ..

# 2. Build and run
docker compose up --build

# 3. Open browser
# http://localhost:5000
```

## Run without Docker

```bash
pip install flask pyspark pandas numpy
cd data && python generate_data.py && cd ..
PYTHONPATH=src python app.py
# Open http://localhost:5000
```

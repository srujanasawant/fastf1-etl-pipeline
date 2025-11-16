# 🏎️ F1 Blush Analytics — Dynamic ETL Pipeline for Unstructured F1 Data

A full-stack Ferrari-blush themed Formula 1 analytics platform featuring:
- **Dynamic ETL pipeline** capable of handling *unstructured and evolving data*
- **FastAPI backend** with complete ETL orchestration, telemetry endpoints, and comparison tools
- **React + Vite frontend** with dashboards, charts, telemetry visualizations, and a blush Ferrari theme
- **MongoDB-ready architecture** for unstructured schema evolution (extendable)

This project was built for the **Dynamic ETL Pipeline Hackathon Problem Statement**.

---

## 🚀 Features

### 🔧 Backend (FastAPI)
- End-to-end ETL pipeline using **FastF1**
- Automatic extraction of laps, drivers, weather, metadata
- Transformation of raw, unstructured data into analytics-friendly formats
- JSON snapshot storage for reproducibility
- Telemetry extraction for:
  - Single lap
  - Full driver stint
- Race comparison endpoint
- Metadata endpoints (seasons, drivers)
- CORS-enabled for frontend access

---

### 🎨 Frontend (React + Vite)
- Ferrari blush-red themed UI
- Pages:
  - **Home** — intro
  - **Dashboard** — session analytics
  - **Compare** — race vs race
  - **Telemetry** — speed/throttle/brake charts
- Components:
  - Driver tables & cards
  - Lap charts
  - Pace charts
  - Weather timeline
  - Telemetry charts
  - Session selector

---

### 🧠 Dynamic Schema ETL
Designed for real-world unstructured data:
- Accepts unpredictable, evolving raw data
- Generates schema structures dynamically during transform stage
- Adaptable storage layer (MongoDB recommended)
- Versioned schema snapshots

---

## 📁 Project Structure

```
f1-etl/
│
├── backend/
│   ├── api/
│   ├── etl/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── pipeline.py
│   ├── schemas/
│   ├── main.py
│   └── requirements.txt
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   ├── pages/
│   │   ├── components/
│   │   ├── utils/
│   │   └── theme/
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   └── .env
│
└── docs/
    ├── api_specs.md
    ├── architecture.md
    ├── data_models.md
    └── roadmap.md
```

---

## ⚙️ Backend Setup & Run

### 1. Create and activate virtual environment
```powershell
cd backend
python -m venv venv
.\venv\Scripts\activate
```

### 2. Install dependencies
```powershell
pip install -r requirements.txt
```

### 3. Start the API server
```powershell
uvicorn main:app --reload
```

API available at:
👉 http://localhost:8000/docs

---

## 💻 Frontend Setup & Run

### 1. Install dependencies
```powershell
cd frontend
npm install
```

### 2. Create `.env`
```
VITE_API_URL=http://localhost:8000/api
```

### 3. Start Vite server
```powershell
npm run dev
```

Frontend available at:
👉 http://localhost:5173/

---

## 📦 Recommended Database for Unstructured Data

For hackathon problem requirements (dynamic schemas, evolving data):

### ✅ **MongoDB** is the best choice.
Because:
- Schema-less storage
- Documents accept ANY structure
- Perfect for ETL pipelines where input data changes shape
- Easy schema versioning

A future version of this project can include:
- Schema snapshots
- Schema evolution tracking
- Raw + transformed storage models

---

## 🧪 Testing the System

After backend + frontend are running:
1. Open **Dashboard** → load a race
2. View drivers, laps, weather
3. Open **Compare** → compare two races
4. Open **Telemetry** → load speed/throttle/brake charts for a lap

---

## 🛠️ ETL Pipeline Flow

```
Raw FastF1 → extract.py → transform.py → load.py → pipeline.py → FastAPI → React UI
```

- `extract.py` loads all raw frames via FastF1
- `transform.py` normalizes, computes analytics, converts to JSON
- `load.py` stores snapshots (Mongo optional)
- `pipeline.py` orchestrates the entire flow

---

## 🗺️ Roadmap
(Also in docs/roadmap.md)

### Phase 1 — Core ETL & UI (DONE)  
### Phase 2 — Advanced analytics (NEXT):
- Stint performance charts
- Tyre degradation curves
- Position delta timeline
- Multiple-lap telemetry overlays

### Phase 3 — Dynamic Schema DB Integration
- Mongo ingestion
- Schema evolution tracking
- Schema diff viewer

### Phase 4 — Deployment
- Backend → Railway/Render
- Frontend → Vercel/Netlify

---

## ❤️ Credits
Created with love, data, and Ferrari blush aesthetic.

For help, improvements, or debugging — just ask!


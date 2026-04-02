# 🚀 AI Data Cleaning + AutoML Platform

A **production-ready, full-stack AI-powered platform** for data cleaning and automated machine learning.

## 🎯 System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AI Data Cleaning + AutoML Platform                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                   │
│   │   Upload    │     │  Database   │     │  API Data   │                   │
│   │   (CSV/XLS) │     │   (SQL)     │     │   (REST)    │                   │
│   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘                   │
│          │                   │                   │                           │
│          └───────────────────┼───────────────────┘                           │
│                              ▼                                               │
│                    ┌─────────────────┐                                       │
│                    │  Multi-Key AI   │                                       │
│                    │    Routing      │  ← gemini-2.5-pro / gpt-5-mini       │
│                    └────────┬────────┘                                       │
│                             ▼                                                │
│                    ┌─────────────────┐                                       │
│                    │  Data Cleaning  │                                       │
│                    │  (Rule-based +  │                                       │
│                    │    AI-powered)  │                                       │
│                    └────────┬────────┘                                       │
│                             ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        AutoML Pipeline                               │   │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │   │
│   │  │Preprocess│→│ Training │→│  Tuning  │→│Leaderboard│→│  Export  │  │   │
│   │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘  │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                             ▼                                                │
│                    ┌─────────────────┐                                       │
│                    │   Inference &   │                                       │
│                    │   Deployment    │                                       │
│                    └─────────────────┘                                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✅ Features Implemented

### 🧹 Data Cleaning
- [x] **Rule-based cleaning** - Duplicates, missing values, outliers
- [x] **AI-powered cleaning** - Context-aware imputation via Euri API
- [x] **Multi-model routing** - Different AI models per data source:
  - Database → `gemini-2.5-pro`
  - Upload → `gpt-5-mini`
  - API → `gemini-2.0-flash`
  - Fallback → `gpt-4.1-mini`

### 🤖 AutoML Pipeline
- [x] **Problem detection** - Auto-detect classification vs regression
- [x] **Target suggestion** - AI-powered target column recommendation
- [x] **9+ Algorithms** - Linear, Ridge, Lasso, RandomForest, XGBoost, SVM, etc.
- [x] **Cross-validation** - With configurable folds
- [x] **Hyperparameter tuning** - Grid Search, Random Search, Optuna
- [x] **Feature importance** - Extraction and visualization
- [x] **Model leaderboard** - Compare and rank trained models

### 📦 Model Management
- [x] **Versioning** - Semantic versioning for all models
- [x] **Export** - Unified `pipeline.pkl` with preprocessing
- [x] **Rollback** - Restore previous model versions
- [x] **Retraining** - Update models with new data

### ⚡ Inference
- [x] **Single prediction** - REST API for individual predictions
- [x] **Batch prediction** - CSV upload for bulk inference
- [x] **Schema validation** - Input validation before prediction
- [x] **Probability output** - For classification problems

### 🔄 Async Training
- [x] **Background jobs** - Non-blocking training
- [x] **Job IDs** - Track training progress
- [x] **Status tracking** - Real-time updates
- [x] **Cancellation** - Stop long-running jobs

### 📊 Observability
- [x] **Structured logging** - JSON logging with levels
- [x] **API key status** - Monitor key health
- [x] **System health** - Service status dashboard
- [x] **Cost governance** - Token estimation, budget limits
- [x] **Drift detection** - Statistical distribution monitoring

### 🔐 Security
- [x] **API key masking** - Secure logging
- [x] **CORS protection** - Configurable origins
- [x] **Environment variables** - Secure configuration

---

## 🏗️ Project Structure

```
AI_Data_Cleaning_Agent/
├── app/
│   ├── flask_app.py          # Flask frontend application
│   ├── ml_routes.py          # ML API routes for frontend
│   └── templates/
│       ├── base.html         # Base template with navigation
│       ├── index.html        # Homepage with feature cards
│       ├── upload.html       # File upload page
│       ├── database.html     # Database query page
│       ├── api_data.html     # API data fetch page
│       ├── ml_dashboard.html # AutoML training wizard
│       ├── inference.html    # Prediction interface
│       ├── tuning.html       # Hyperparameter tuning UI
│       ├── model_versions.html # Version management
│       └── observability.html  # Monitoring dashboard
│
├── scripts/
│   ├── backend.py            # FastAPI backend server
│   ├── ai_agent.py           # AI-powered data cleaning
│   ├── euri_client.py        # Multi-key API client
│   └── data_cleaning.py      # Rule-based cleaning
│
├── services/
│   ├── ml_pipeline.py        # Core ML training pipeline
│   ├── preprocessing.py      # Data preprocessing
│   ├── hyperparameter_tuning.py # Tuning algorithms
│   ├── model_manager.py      # Version management
│   ├── explainability.py     # SHAP integration
│   ├── observability.py      # Logging & metrics
│   ├── async_training.py     # Background job system
│   ├── data_validation.py    # Input validation
│   └── feature_engineering.py # Feature creation
│
├── api/
│   ├── routes.py             # FastAPI route definitions
│   └── schemas.py            # Pydantic schemas
│
├── .env                      # Environment configuration
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd AI_Data_Cleaning_Agent
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file with your API keys:

```env
# API Keys for Multi-Model Routing
EURI_API_KEY_DATABASE=your-gemini-key
EURI_API_KEY_UPLOAD=your-gpt-key
EURI_API_KEY_API=your-gemini-flash-key
EURI_API_KEY_FALLBACK=your-fallback-key

# Model Configuration
EURI_MODEL_DATABASE=gemini-2.5-pro
EURI_MODEL_UPLOAD=gpt-5-mini
EURI_MODEL_API=gemini-2.0-flash
EURI_MODEL_FALLBACK=gpt-4.1-mini

# Server Configuration
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
FLASK_HOST=127.0.0.1
FLASK_PORT=5000
```

### 3. Start the Backend

```bash
python scripts/backend.py
```

Backend will be available at: `http://127.0.0.1:8000`

### 4. Start the Frontend

In a new terminal:

```bash
python app/flask_app.py
```

Frontend will be available at: `http://127.0.0.1:5000`

### 5. Access the Platform

Open your browser to: **http://127.0.0.1:5000**

---

## 📖 Usage Guide

### Data Cleaning

1. **Upload File**: Go to "Data Cleaning" → "Upload File"
2. Select a CSV or Excel file
3. Click "Clean Data" - AI will process automatically
4. Download the cleaned dataset

### AutoML Training

1. **Start Training**: Go to "AutoML" → "ML Dashboard"
2. **Step 1**: Upload your training data
3. **Step 2**: Select target column (AI suggests best options)
4. **Step 3**: Choose algorithms to compare
5. **Step 4**: Watch training progress
6. **Step 5**: View leaderboard and export best model

### Making Predictions

1. Go to "AutoML" → "Inference"
2. For single predictions: Fill in feature values
3. For batch predictions: Upload a CSV file
4. View predictions and probabilities

### Hyperparameter Tuning

1. Go to "AutoML" → "Tuning"
2. Select algorithm and tuning method
3. Configure number of trials
4. Start tuning and view best parameters

---

## 🔌 API Endpoints

### Backend (FastAPI) - Port 8000

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/clean-data` | POST | Clean uploaded file |
| `/clean-db` | POST | Clean database query |
| `/clean-api` | POST | Clean API data |
| `/api/v2/upload-data` | POST | Upload training data |
| `/api/v2/select-target` | POST | Set target column |
| `/api/v2/train-model` | POST | Train single model |
| `/api/v2/compare-models` | POST | Train multiple models |
| `/api/v2/tune-model` | POST | Hyperparameter tuning |
| `/api/v2/mlops/save-model` | POST | Save model version |
| `/api/v2/mlops/predict` | POST | Make predictions |
| `/api/v2/health` | GET | Detailed health check |

### Frontend (Flask) - Port 5000

| Page | URL | Description |
|------|-----|-------------|
| Home | `/` | Landing page |
| Upload | `/upload` | File upload |
| Database | `/database` | SQL query |
| API Data | `/api_data` | REST API fetch |
| ML Dashboard | `/ml_dashboard` | Training wizard |
| Inference | `/inference` | Predictions |
| Tuning | `/tuning` | Hyperparameters |
| Model Versions | `/model_versions` | Version management |

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|------------|
| **Frontend** | Flask, Bootstrap 5, jQuery, Chart.js |
| **Backend** | FastAPI, Uvicorn, Pydantic |
| **ML** | Scikit-learn, XGBoost, Pandas, NumPy |
| **AI** | Euri API (GPT, Gemini models) |
| **Database** | PostgreSQL, SQLAlchemy |
| **Async** | Python asyncio, threading |

---

## 📊 Supported Algorithms

### Classification
- Logistic Regression
- Random Forest Classifier
- Support Vector Machine (SVM)
- XGBoost Classifier

### Regression
- Linear Regression
- Ridge Regression
- Lasso Regression
- Random Forest Regressor
- XGBoost Regressor

---

## 🔒 Security Notes

1. **Never commit `.env` files** - They contain sensitive API keys
2. **Use strong secret keys** in production
3. **Enable HTTPS** for production deployments
4. **Review CORS settings** before deployment

---

## 📝 License

MIT License - See LICENSE file for details.

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## 📧 Support

For questions or issues, please open a GitHub issue.

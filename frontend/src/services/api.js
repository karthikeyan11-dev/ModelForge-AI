import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v2',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request Interceptor: Attach token if it exists
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
}, (error) => {
  return Promise.reject(error);
});

// Response Interceptor: Handle 401 errors
api.interceptors.response.use((response) => {
  return response;
}, (error) => {
  if (error.response?.status === 401) {
    // Prevent redirect loop if we're already on login/register
    const isAuthPath = window.location.pathname === '/login' || window.location.pathname === '/register';
    if (!isAuthPath) {
      localStorage.removeItem('token');
      window.location.href = '/login';
    }
  }
  return Promise.reject(error);
});

export const apiService = {
  // --- Ingestion Data Sources ---
  uploadDataset: async (file, onProgress) => {
    const formData = new FormData();
    formData.append('file', file);
    const { data } = await api.post('/upload-data', formData, {
      headers: { 'Content-Type': undefined },
      onUploadProgress: (p) => {
        if (onProgress && p.total) {
          onProgress(Math.round((p.loaded * 100) / p.total));
        }
      }
    });
    return data;
  },
  previewAPIData: async (apiUrl) => (await api.post('/ingest/api/preview', { api_url: apiUrl })).data,
  ingestAPIData: async (apiUrl, name) => (await api.post('/ingest/api', { api_url: apiUrl, dataset_name: name })).data,

  // --- Training & Jobs ---
  runTraining: async (payload) => (await api.post('/train-model', payload)).data,
  getTrainingStatus: async (jobId) => (await api.get(`/jobs/${jobId}`)).data,
  getLeaderboard: async () => (await api.get('/mlops/versions/default')).data,

  // --- Benchmarks ---
  runBenchmark: async (payload) => (await api.post('/mlops/benchmark/run', payload)).data,
  getBenchmarkStatus: async (jobId) => (await api.get(`/mlops/benchmark/status/${jobId}`)).data,
  getBenchmarkResults: async (jobId) => (await api.get(`/mlops/benchmark/results/${jobId}`)).data,

  // --- Preprocessing & Lifecycle ---
  getDatasets: async () => (await api.get('/datasets')).data,
  startPreprocessing: async (datasetId, options) => (await api.post(`/preprocess/${datasetId}`, { options })).data,
  getPreprocessingStatus: async (jobId) => (await api.get(`/preprocess-status/${jobId}`)).data,
  getDatasetVersions: async (datasetId) => (await api.get(`/datasets/${datasetId}/versions`)).data,

  // --- Explainability ---
  getExplainabilityGlobal: async () => (await api.get('/mlops/explain/global')).data,
  getExplainabilityNarrative: async (importances) => (await api.post('/mlops/explain/narrative', { feature_importance: importances })).data,
};

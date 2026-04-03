import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { 
  UploadCloud, 
  CheckCircle, 
  AlertCircle, 
  Loader2, 
  Link as LinkIcon, 
  Database,
  Search,
  Save,
  FileText
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';

const UploadPage = () => {
  const [ingestMode, setIngestMode] = useState('upload'); // 'upload' or 'api'
  const [file, setFile] = useState(null);
  const [apiUrl, setApiUrl] = useState('');
  const [apiPreview, setApiPreview] = useState(null);
  const [datasetName, setDatasetName] = useState('');
  const [progress, setProgress] = useState(0);
  const [notification, setNotification] = useState(null);
  
  const navigate = useNavigate();

  // --- Mutations ---
  const uploadMutation = useMutation({
    mutationFn: (uploadFile) => apiService.uploadDataset(uploadFile, setProgress),
    onSuccess: (data) => {
      setNotification({ type: 'success', message: 'Dataset ingested successfully!' });
      localStorage.setItem('dataset_id', data.dataset_id);
      setTimeout(() => navigate('/preprocess'), 1500); 
    },
  });

  const previewMutation = useMutation({
    mutationFn: (url) => apiService.previewAPIData(url),
    onSuccess: (data) => setApiPreview(data),
  });

  const apiIngestMutation = useMutation({
    mutationFn: () => apiService.ingestAPIData(apiUrl, datasetName),
    onSuccess: (data) => {
      localStorage.setItem('dataset_id', data.dataset_id);
      navigate('/preprocess');
    },
  });

  const handleIngest = () => {
    if (ingestMode === 'upload' && file) {
      uploadMutation.mutate(file);
    } else if (ingestMode === 'api' && datasetName) {
      apiIngestMutation.mutate();
    }
  };

  return (
    <div className="max-w-5xl mx-auto py-10 px-6">
      <div className="text-center mb-8">
        <h1 className="text-4xl font-extrabold text-gray-900 tracking-tight flex items-center justify-center">
          <Database className="mr-3 h-10 w-10 text-indigo-600" />
          Data Ingestion Hub
        </h1>
        <p className="mt-4 text-lg text-gray-600">Securely ingest datasets from files or external cloud APIs.</p>
      </div>

      {/* Source Selector Tabs */}
      <div className="flex justify-center mb-8 p-1 bg-gray-100 rounded-xl w-fit mx-auto shadow-inner">
        <button 
          onClick={() => setIngestMode('upload')}
          className={`flex items-center px-6 py-2 rounded-lg text-sm font-bold transition-all ${
            ingestMode === 'upload' ? 'bg-white text-indigo-600 shadow-sm' : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <UploadCloud className="mr-2 h-4 w-4" />
          Local File
        </button>
        <button 
          onClick={() => setIngestMode('api')}
          className={`flex items-center px-6 py-2 rounded-lg text-sm font-bold transition-all ${
            ingestMode === 'api' ? 'bg-white text-indigo-600 shadow-sm' : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <LinkIcon className="mr-2 h-4 w-4" />
          External API
        </button>
      </div>

      {/* Mode Content */}
      <div className="bg-white rounded-2xl shadow-xl border border-gray-100 overflow-hidden min-h-[400px] flex flex-col">
        {ingestMode === 'upload' ? (
          <div 
            className={`flex-1 m-6 border-2 border-dashed rounded-2xl flex flex-col items-center justify-center transition-all
              ${file ? 'border-indigo-500 bg-indigo-50' : 'border-gray-200 hover:border-gray-300 bg-gray-50/50 hover:bg-gray-50'}`}
            onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); }}
            onDrop={(e) => {
              e.preventDefault(); e.stopPropagation();
              if (e.dataTransfer.files?.[0]) setFile(e.dataTransfer.files[0]);
            }}
          >
            <div className="space-y-4 text-center p-12">
              <UploadCloud className={`mx-auto h-20 w-20 ${file ? 'text-indigo-500' : 'text-gray-300'}`} />
              <div className="flex text-xl text-gray-600 justify-center font-medium">
                <label className="relative cursor-pointer text-indigo-600 hover:text-indigo-700">
                  <span>Choose file</span>
                  <input type="file" className="sr-only" onChange={(e) => setFile(e.target.files[0])} />
                </label>
                <span className="pl-2">or drag and drop</span>
              </div>
              <p className="text-sm text-gray-400">CSV, XLSX, JSON up to 100MB</p>
              {file && (
                <div className="mt-4 flex items-center justify-center p-3 bg-white border border-indigo-100 rounded-lg shadow-sm">
                  <FileText className="h-5 w-5 text-indigo-500 mr-2" />
                  <span className="font-bold text-gray-700">{file.name}</span>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="p-8 flex flex-col flex-1">
            <div className="flex space-x-4 mb-6">
              <div className="relative flex-1">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <LinkIcon className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="text"
                  placeholder="https://api.example.com/data"
                  value={apiUrl}
                  onChange={(e) => setApiUrl(e.target.value)}
                  className="block w-full pl-10 pr-3 py-4 border border-gray-200 rounded-xl leading-5 bg-gray-50 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all font-medium"
                />
              </div>
              <button
                onClick={() => previewMutation.mutate(apiUrl)}
                disabled={!apiUrl || previewMutation.isPending}
                className="px-8 py-4 bg-indigo-600 text-white rounded-xl font-bold shadow-lg shadow-indigo-200 hover:bg-indigo-700 disabled:bg-gray-400 flex items-center"
              >
                {previewMutation.isPending ? <Loader2 className="animate-spin h-5 w-5 mr-2" /> : <Search className="h-5 w-5 mr-2" />}
                Fetch Preview
              </button>
            </div>

            {apiPreview && (
              <div className="flex-1 animate-in fade-in slide-in-from-bottom-4 duration-300">
                <div className="mb-4 flex items-center justify-between">
                   <h3 className="text-lg font-bold text-gray-800 flex items-center">
                     <CheckCircle className="h-5 w-5 text-green-500 mr-2" />
                     Row Preview Found ({apiPreview.row_count} rows)
                   </h3>
                </div>
                <div className="overflow-x-auto border border-gray-100 rounded-xl mb-8 max-h-60 shadow-inner">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        {Object.keys(apiPreview.preview[0] || {}).map(k => (
                          <th key={k} className="px-4 py-3 text-left text-[10px] font-black text-gray-400 uppercase tracking-widest">{k}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-100">
                      {apiPreview.preview.map((row, i) => (
                        <tr key={i}>
                          {Object.values(row).map((v, j) => (
                            <td key={j} className="px-4 py-3 whitespace-nowrap text-sm text-gray-600 font-medium">{String(v).substring(0, 20)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="p-6 bg-indigo-50 rounded-2xl border border-indigo-100">
                   <label className="block text-sm font-black text-indigo-700 uppercase tracking-widest mb-3">Dataset Identity</label>
                   <div className="flex space-x-4">
                     <input
                       type="text"
                       placeholder="Enter unique dataset name (e.g. Sales_Q1)"
                       value={datasetName}
                       onChange={(e) => setDatasetName(e.target.value)}
                       className="flex-1 px-5 py-4 border border-indigo-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none font-bold text-gray-700"
                     />
                   </div>
                   <p className="mt-2 text-xs text-indigo-400 font-black flex items-center">
                     <AlertCircle className="h-3 w-3 mr-1" />
                     MANDATORY CSV CONVERSION FOR STORAGE
                   </p>
                </div>
              </div>
            )}
            
            {previewMutation.isError && (
              <div className="mt-4 p-4 bg-red-50 border border-red-100 rounded-xl text-red-700 flex items-center">
                <AlertCircle className="h-5 w-5 mr-3" />
                {previewMutation.error.response?.data?.detail || "Failed to reach endpoint."}
              </div>
            )}
          </div>
        )}

        {/* Action Bar */}
        <div className="p-6 bg-gray-50 border-t border-gray-100 flex justify-between items-center">
          <div className="flex-1 max-w-md">
            {(uploadMutation.isPending) && (
              <div className="w-full bg-gray-200 rounded-full h-2 shadow-inner">
                <div className="bg-indigo-600 h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
              </div>
            )}
            {notification && (
              <p className={`text-xs font-bold uppercase tracking-widest flex items-center ${notification.type === 'warning' ? 'text-amber-600' : 'text-indigo-600'}`}>
                <Sparkles className="h-4 w-4 mr-1 animate-pulse" />
                {notification.message}
              </p>
            )}
          </div>
          <button
            onClick={handleIngest}
            disabled={
                (ingestMode === 'upload' && !file) || 
                (ingestMode === 'api' && !datasetName) ||
                uploadMutation.isPending || 
                apiIngestMutation.isPending
            }
            className="flex items-center px-10 py-4 border border-transparent text-lg font-black rounded-xl shadow-2xl text-white bg-gray-900 hover:bg-black disabled:bg-gray-400 transition-all transform active:scale-95"
          >
            {uploadMutation.isPending || apiIngestMutation.isPending ? (
              <Loader2 className="animate-spin h-5 w-5 mr-3" />
            ) : <Save className="h-5 w-5 mr-3" />}
            {ingestMode === 'upload' ? 'Ingest Artifact' : 'Persist API Dataset'}
          </button>
        </div>
      </div>
    </div>
  );
};

const Sparkles = (props) => (
    <svg {...props} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" />
        <path d="m5 3 1 1" /><path d="m19 19 1 1" /><path d="m5 21 1-1" /><path d="m19 3 1-1" />
    </svg>
)

export default UploadPage;

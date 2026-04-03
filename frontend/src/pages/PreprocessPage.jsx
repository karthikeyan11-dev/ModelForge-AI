import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { 
  Database, 
  FileText, 
  Settings2, 
  CheckCircle2, 
  AlertCircle, 
  Loader2, 
  Play,
  History,
  ShieldCheck,
  ChevronRight,
  DatabaseZap,
  Layers,
  Sparkles
} from 'lucide-react';

const PreprocessPage = () => {
  const queryClient = useQueryClient();
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [options, setOptions] = useState({
    handle_missing: true,
    remove_duplicates: true,
    encode_categorical: true,
    scale_numerical: false,
    remove_outliers: false,
    automated: true
  });
  const [taskId, setTaskId] = useState(null);
  const [polling, setPolling] = useState(false);
  const [showAllVersions, setShowAllVersions] = useState(false);
  const [results, setResults] = useState(null);

  // 1. Fetch Datasets
  const { data: datasets, isLoading, error } = useQuery({
    queryKey: ['datasets'],
    queryFn: () => apiService.getDatasets(),
    refetchInterval: polling ? 3000 : false 
  });

  // 2. Preprocess Mutation
  const preprocessMutation = useMutation({
    mutationFn: ({ id, options }) => apiService.startPreprocessing(id, options),
    onSuccess: (data) => {
      setTaskId(data.task_id);
      setPolling(true);
      setResults(null);
      queryClient.invalidateQueries(['datasets']);
    }
  });

  // 3. Status Polling Task
  useEffect(() => {
    let interval;
    if (polling && taskId) {
      interval = setInterval(async () => {
        try {
          const res = await apiService.getPreprocessingStatus(taskId);
          if (res.status === 'completed') {
            setPolling(false);
            setTaskId(null);
            setResults(res.result);
            queryClient.invalidateQueries(['datasets']);
          } else if (res.status === 'failed') {
            setPolling(false);
            setTaskId(null);
            console.error("Task failed:", res.error);
          }
        } catch (err) {
          console.error("Polling error", err);
          setPolling(false);
        }
      }, 3000);
    }
    return () => clearInterval(interval);
  }, [polling, taskId, queryClient]);

  const handleStart = () => {
    if (!selectedDataset) return;
    preprocessMutation.mutate({ id: selectedDataset.id, options });
  };

  if (isLoading) return (
    <div className="flex items-center justify-center h-full">
      <Loader2 className="h-12 w-12 animate-spin text-indigo-600" />
    </div>
  );

  return (
    <div className="p-8 max-w-7xl mx-auto flex flex-col h-full overflow-hidden">
      <div className="flex justify-between items-end mb-8">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 mb-2 flex items-center">
            <DatabaseZap className="mr-3 h-8 w-8 text-indigo-600" />
            Data Preparation
          </h1>
          <p className="text-gray-500">Enable parallel Versioning & Lineage Tracking.</p>
        </div>
        
        <div className="flex items-center space-x-4">
            <button 
                onClick={() => setShowAllVersions(!showAllVersions)}
                className={`flex items-center px-4 py-2 rounded-lg text-sm font-bold transition-all ${
                    showAllVersions 
                    ? 'bg-indigo-600 text-white shadow-md' 
                    : 'bg-white text-gray-600 border border-gray-200 hover:bg-gray-50'
                }`}
            >
                <Layers className="h-4 w-4 mr-2" />
                {showAllVersions ? 'Viewing All Versions' : 'View History'}
            </button>
            <div className="flex items-center space-x-2 bg-indigo-50 border border-indigo-100 rounded-lg px-4 py-2">
                <History className="h-5 w-5 text-indigo-600" />
                <span className="text-sm font-semibold text-indigo-700">{datasets?.length || 0} Artifacts</span>
            </div>
        </div>
      </div>

      <div className="flex flex-1 gap-8 overflow-hidden">
        {/* Dataset List */}
        <div className="flex-1 overflow-y-auto pr-4 space-y-4">
          {datasets?.length === 0 ? (
            <div className="bg-white border-2 border-dashed rounded-xl p-12 text-center">
              <FileText className="h-16 w-16 text-gray-300 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-900">No datasets found</h3>
              <p className="text-gray-500 mt-1">Upload a dataset to start the versioning lifecycle.</p>
            </div>
          ) : (
            datasets?.map((ds) => (
              <div 
                key={ds.id}
                onClick={() => !polling && setSelectedDataset(ds)}
                className={`group relative bg-white border rounded-xl p-5 cursor-pointer transition-all hover:shadow-lg ${
                  selectedDataset?.id === ds.id ? 'ring-2 ring-indigo-500 border-indigo-200' : 'border-gray-200 shadow-sm'
                } ${ds.status === 'processing' ? 'opacity-80' : ''}`}
              >
                <div className="flex items-center">
                  <div className={`h-12 w-12 rounded-lg flex items-center justify-center mr-4 transition-colors ${
                    ds.status === 'processed' ? 'bg-green-100 text-green-600' : 'bg-indigo-100 text-indigo-600'
                  }`}>
                    {ds.is_latest ? <Sparkles className="h-6 w-6" /> : <Database className="h-6 w-6" />}
                  </div>
                  
                  <div className="flex-1">
                    <div className="flex items-center space-x-2">
                        <h3 className="font-bold text-gray-900 group-hover:text-indigo-600 transition-colors uppercase tracking-tight">
                            {ds.name}
                        </h3>
                        <span className="text-[10px] font-black bg-gray-900 text-white px-2 py-0.5 rounded uppercase">
                            v{ds.version}
                        </span>
                    </div>
                    <div className="flex items-center text-xs text-gray-400 mt-1 space-x-3 font-medium">
                      <span>{(ds.file_size / 1024).toFixed(2)} KB</span>
                      <span>•</span>
                      <span>{new Date(ds.created_at).toLocaleTimeString()}</span>
                      <span>•</span>
                      <span>{ds.col_count || '0'} Features</span>
                    </div>
                  </div>

                  <div className="flex items-center">
                    {ds.is_latest && (
                      <span className="mr-3 flex items-center text-[10px] font-black text-indigo-700 bg-indigo-100 px-2 py-1 rounded border border-indigo-200">
                        LATEST
                      </span>
                    )}
                    {ds.status === 'processed' ? (
                      <span className="flex items-center text-xs font-bold text-green-600 bg-green-50 px-2 py-1 rounded-md border border-green-200">
                        <CheckCircle2 className="h-3 w-3 mr-1" />
                        READY
                      </span>
                    ) : ds.status === 'processing' ? (
                      <span className="flex items-center text-xs font-bold text-indigo-600 bg-indigo-50 px-2 py-1 rounded-md border border-indigo-200">
                        <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                        SYNCING
                      </span>
                    ) : (
                      <span className="text-xs font-bold text-gray-500 bg-gray-100 px-2 py-1 rounded-md border border-gray-200">
                        BASE
                      </span>
                    )}
                    <ChevronRight className={`ml-4 h-5 w-5 text-gray-300 group-hover:text-indigo-500 group-hover:translate-x-1 transition-all ${selectedDataset?.id === ds.id ? 'text-indigo-500' : ''}`} />
                  </div>
                </div>
              </div>
            ))
          )}
        </div>

        {/* Configuration Panel */}
        <div className="w-96 flex-shrink-0">
          {selectedDataset ? (
            <div className="bg-white border border-gray-200 rounded-2xl shadow-xl p-6 h-fit sticky top-0 transition-all">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center ">
                  <Settings2 className="h-6 w-6 text-indigo-600 mr-2" />
                  <h2 className="text-xl font-bold text-gray-900 tracking-tight">Version Controls</h2>
                </div>
                <span className="text-xs font-black text-gray-400">ID: {selectedDataset.id.substring(0,8)}</span>
              </div>

              <div className="space-y-6">
                <div className="bg-gray-50 border border-gray-200 rounded-xl p-4">
                    <p className="text-xs font-black text-gray-400 uppercase tracking-widest mb-2">Current Context</p>
                    <p className="text-sm font-bold text-gray-700">{selectedDataset.name}</p>
                    <div className="mt-2 flex items-center justify-between">
                        <span className="text-xs font-medium text-gray-500">Current Revision</span>
                        <span className="text-xs font-black text-indigo-600">v{selectedDataset.version}</span>
                    </div>
                </div>

                <div className="space-y-4">
                  <ConfigToggle 
                    label="Automated Intelligent Pipeline" 
                    desc="Self-analyzing preprocessing"
                    checked={options.automated} 
                    onChange={(v) => setOptions({...options, automated: v})} 
                    highlight={true}
                  />
                  {!options.automated && (
                    <div className="space-y-4 pt-2 border-t border-gray-100 mt-2">
                        <ConfigToggle 
                            label="Handle missing values" 
                            desc="Numerical & Categorical imputation"
                            checked={options.handle_missing} 
                            onChange={(v) => setOptions({...options, handle_missing: v})} 
                        />
                        <ConfigToggle 
                            label="Encode categorical" 
                            desc="One-Hot encoding for strings"
                            checked={options.encode_categorical} 
                            onChange={(v) => setOptions({...options, encode_categorical: v})} 
                        />
                        <ConfigToggle 
                            label="Scale numerical" 
                            desc="StandardScaler (Z-score)"
                            checked={options.scale_numerical} 
                            onChange={(v) => setOptions({...options, scale_numerical: v})} 
                        />
                        <ConfigToggle 
                            label="Remove outliers" 
                            desc="IQR-based clipping"
                            checked={options.remove_outliers} 
                            onChange={(v) => setOptions({...options, remove_outliers: v})} 
                        />
                    </div>
                  )}
                </div>

                {results && (
                  <div className="bg-green-50 border border-green-100 rounded-xl p-4 animate-in fade-in duration-500">
                    <p className="text-xs font-black text-green-700 uppercase tracking-widest mb-2">Preprocessing Applied</p>
                    <div className="flex flex-wrap gap-2">
                        {results.steps_applied?.map(step => (
                            <span key={step} className="text-[10px] font-bold bg-green-200 text-green-800 px-2 py-0.5 rounded uppercase">
                                {step.replace(/_/g, ' ')}
                            </span>
                        ))}
                    </div>
                    <div className="mt-2 text-[10px] text-green-600 font-medium italic">
                        Processing time: {results.processing_time_ms?.toFixed(0)}ms
                    </div>
                  </div>
                )}

                <div className="pt-6 border-t border-gray-100">
                  <button
                    onClick={handleStart}
                    disabled={preprocessMutation.isPending || polling}
                    className={`w-full py-4 px-6 rounded-xl flex items-center justify-center font-bold text-white shadow-lg transition-all transform active:scale-95 ${
                      preprocessMutation.isPending || polling
                        ? 'bg-gray-400 cursor-not-allowed'
                        : 'bg-indigo-600 hover:bg-indigo-700 hover:-translate-y-1'
                    }`}
                  >
                    {preprocessMutation.isPending || polling ? (
                      <Loader2 className="h-6 w-6 animate-spin mr-2" />
                    ) : (
                      <Play className="h-5 w-5 mr-2 fill-current" />
                    )}
                    {polling ? 'ENGINE ANALYZING...' : `PREPROCESS DATA`}
                  </button>
                  <div className="mt-4 flex items-center justify-center text-xs text-gray-400 space-x-4">
                    <div className="flex items-center">
                        <ShieldCheck className="h-3 w-3 mr-1" />
                        Multi-tenant
                    </div>
                    <div className="flex items-center">
                        <Sparkles className="h-3 w-3 mr-1 text-indigo-400" />
                        Parallel Execution
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="h-full border-2 border-dashed border-gray-200 rounded-2xl flex flex-col items-center justify-center p-8 text-center text-gray-400">
               <Layers className="h-12 w-12 mb-4 opacity-50" />
               <p className="font-medium">Direct lineage selection required</p>
               <p className="text-xs mt-2">Choose an artifact to branch from</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const ConfigToggle = ({ label, desc, checked, onChange, highlight = false }) => (
  <div 
    onClick={() => onChange(!checked)}
    className={`p-3 rounded-xl border-2 transition-all cursor-pointer ${
      checked 
        ? (highlight ? 'bg-indigo-600 border-indigo-700 shadow-lg' : 'bg-indigo-50 border-indigo-200') 
        : 'bg-gray-50 border-transparent hover:border-gray-200'
    }`}
  >
    <div className="flex items-center justify-between">
      <div>
        <span className={`block text-sm font-bold ${checked ? (highlight ? 'text-white' : 'text-indigo-700') : 'text-gray-700'}`}>{label}</span>
        <span className={`text-[10px] uppercase font-black tracking-widest ${checked && highlight ? 'text-indigo-200' : 'text-gray-400'}`}>{desc}</span>
      </div>
      <div className={`w-10 h-5 rounded-full p-0.5 transition-colors ${checked ? (highlight ? 'bg-white/20' : 'bg-indigo-600') : 'bg-gray-300'}`}>
        <div className={`bg-white w-4 h-4 rounded-full shadow-md transform transition-transform ${checked ? 'translate-x-5' : 'translate-x-0'}`} />
      </div>
    </div>
  </div>
);

export default PreprocessPage;

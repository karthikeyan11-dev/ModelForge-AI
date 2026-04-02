import React, { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { Activity, Play, PlusCircle, CheckCircle, Database } from 'lucide-react';

const DashboardPage = () => {
  const [modelType, setModelType] = useState('random_forest');
  const datasetId = localStorage.getItem('dataset_id') || 'Unknown';

  const { data: statusData, refetch } = useQuery({
    queryKey: ['trainingStatus', datasetId],
    queryFn: () => apiService.getTrainingStatus('latest'),
    refetchInterval: (data) => data?.status === 'running' ? 2000 : false,
  });

  const mutation = useMutation({
    mutationFn: (payload) => apiService.runTraining(payload),
    onSuccess: () => refetch(),
  });

  return (
    <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
      <h3 className="text-3xl font-extrabold text-gray-900 border-b pb-5 mb-8 flex items-center">
        <Activity className="mr-3 text-indigo-600 w-8 h-8" /> Training Control Center
      </h3>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm col-span-2">
          <h4 className="text-lg font-bold text-gray-800 mb-4 flex items-center">
            <Database className="w-5 h-5 mr-2 text-gray-500" /> Active Dataset Configuration
          </h4>
          <div className="bg-gray-50 rounded p-4 text-sm text-gray-700 font-mono">
            Dataset Session: <span className="font-semibold text-indigo-600">{datasetId}</span>
          </div>
          
          <div className="mt-6 flex flex-col gap-4">
            <label className="text-sm font-semibold text-gray-700">Select Backbone Algorithm</label>
            <select 
               className="border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 w-full sm:w-1/2 p-2 px-3 border"
               value={modelType}
               onChange={(e) => setModelType(e.target.value)}
            >
              <option value="random_forest">Random Forest (Stable)</option>
              <option value="xgboost">XGBoost (High Performance)</option>
              <option value="linear_regression">Linear Regression (Baseline)</option>
              <option value="svm">Support Vector Machine (Complex Boundaries)</option>
            </select>
          </div>

          <button 
            onClick={() => mutation.mutate({ algorithm: modelType, cv_folds: 5, hyperparameters: {} })}
            disabled={mutation.isPending || statusData?.status === 'running'}
            className="mt-6 inline-flex items-center px-5 py-2.5 border border-transparent text-sm font-medium rounded-lg text-white bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-400 gap-2 transition-all"
          >
            <Play className="w-4 h-4" /> Execute Training Pipeline
          </button>
        </div>

        <div className="bg-gradient-to-br from-indigo-50 to-white p-6 rounded-lg border border-indigo-100 shadow-sm">
          <h4 className="text-lg font-bold text-gray-800 mb-4 flex items-center">
            <PlusCircle className="w-5 h-5 mr-2 text-indigo-500" /> Live Pipeline Status
          </h4>
          
          <div className="mt-4 flex flex-col gap-3">
             <div className="flex justify-between text-sm">
                <span className="text-gray-500">Current Phase:</span>
                <span className="font-semibold capitalize text-indigo-600">{statusData?.status || 'Idle'}</span>
             </div>
             <div className="flex justify-between text-sm">
                <span className="text-gray-500">Epochs / Folds:</span>
                <span className="font-semibold">5 / 5 CV</span>
             </div>
             {statusData?.status === 'completed' && (
                <div className="mt-4 p-3 bg-green-50 text-green-700 text-sm rounded flex items-center gap-2 border border-green-200">
                  <CheckCircle className="w-5 h-5"/> Model compiled and saved successfully. View leaderboard for metric breakdown.
                </div>
             )}
          </div>
        </div>
      </div>
    </div>
  );
};
export default DashboardPage;

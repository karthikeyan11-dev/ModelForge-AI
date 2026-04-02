import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { Cpu, Eye, Info, PenTool, Loader2 } from 'lucide-react';

const ExplainabilityPage = () => {
  const { data: globalShap, isLoading: globalLoading } = useQuery({
    queryKey: ['shapExplainers'],
    queryFn: () => apiService.getExplainabilityGlobal()
  });

  const { data: narrative, isLoading: narrativeLoading, isError } = useQuery({
    queryKey: ['narrativeShap', globalShap?.feature_importance],
    queryFn: () => apiService.getExplainabilityNarrative(globalShap?.feature_importance),
    enabled: !!globalShap?.feature_importance && Object.keys(globalShap.feature_importance).length > 0
  });

  return (
    <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
      <div className="mb-8 border-b border-gray-200 pb-5">
        <h3 className="text-3xl font-extrabold text-gray-900 flex items-center gap-3">
          <Cpu className="text-rose-500 w-8 h-8" /> SHAP Explainability Engine
        </h3>
        <p className="mt-2 max-w-4xl text-sm text-gray-500">Feature Importance Visualizations natively mapped using LLMs.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        {/* Visual Charts Component */}
        <div className="bg-white p-6 shadow-sm rounded-xl border border-gray-200">
           <h4 className="flex items-center text-lg font-bold text-gray-800 mb-6 border-b pb-2"><Eye className="text-indigo-500 mr-2 w-5 h-5"/> Global Importance Weights</h4>
           {globalLoading ? (
              <Loader2 className="animate-spin w-12 h-12 text-indigo-600 mx-auto my-12" />
           ) : !globalShap?.feature_importance ? (
              <p className="text-gray-500 text-sm py-4 text-center">Train an active model to generate SHAP visualizations safely.</p>
           ) : (
              <div className="space-y-4 max-h-96 overflow-y-auto pr-2 custom-scrollbar">
                {Object.entries(globalShap.feature_importance).sort((a,b)=>b[1]-a[1]).map(([feature, weight], idx) => (
                   <div key={feature} className="w-full">
                     <div className="flex justify-between text-sm font-semibold text-gray-700 mb-1">
                       <span>{feature}</span>
                       <span>{(weight * 100).toFixed(2)}%</span>
                     </div>
                     <div className="w-full bg-gray-100 rounded-full h-3">
                        <div className="bg-indigo-500 h-3 rounded-full transition-all" style={{ width: `${Math.min(weight * 100, 100)}%` }}></div>
                     </div>
                   </div>
                ))}
              </div>
           )}
        </div>

        {/* Narrative Engine */}
        <div className="bg-white p-6 shadow-sm rounded-xl border border-gray-200 bg-gradient-to-br from-indigo-50/50 to-white relative overflow-hidden">
           <h4 className="flex items-center text-lg font-bold text-indigo-900 mb-6"><PenTool className="text-indigo-600 mr-2 w-5 h-5"/> AI Plain English Analysis</h4>
           {narrativeLoading ? (
              <div className="flex flex-col justify-center items-center py-10">
                 <Loader2 className="animate-spin w-8 h-8 text-indigo-400 mb-3" />
                 <span className="text-indigo-600 font-medium text-sm">Consulting AI Knowledge Graphs...</span>
              </div>
           ) : narrative?.explanation ? (
              <div className="prose prose-indigo max-w-none text-gray-700 leading-relaxed space-y-4">
                 <p className="font-medium bg-white p-4 border rounded-md shadow-sm border-indigo-100 whitespace-pre-wrap">{narrative.explanation}</p>
                 <div className="flex items-start text-xs text-indigo-500 pt-4 mt-4 border-t border-indigo-100">
                   <Info className="w-4 h-4 mr-1 shrink-0" /> SHAP narrative generations leverage multi-parameter localized AI insights natively decoupled locally.
                 </div>
              </div>
           ) : (
              <p className="text-gray-400 italic text-sm text-center my-10 border border-dashed rounded p-5 bg-white">
                No active SHAP parameters detected for narrative extraction.
              </p>
           )}
        </div>
      </div>
    </div>
  );
};
export default ExplainabilityPage;

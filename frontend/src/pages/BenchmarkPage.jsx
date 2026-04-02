import React, { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { TrendingUp, FilePlus, Loader2, GaugeCircle, Play} from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const BenchmarkPage = () => {
  const [jobId, setJobId] = useState(null);
  
  const handleRun = useMutation({
    mutationFn: () => apiService.runBenchmark({ target_column: "target" }),
    onSuccess: (data) => setJobId(data.job_id),
  });

  const { data: statusData, isLoading: statusLoading } = useQuery({
    queryKey: ['benchmarkStatus', jobId],
    queryFn: () => apiService.getBenchmarkStatus(jobId),
    enabled: !!jobId && (jobId !== 'completed'),
    refetchInterval: (data) => data?.status === 'running' ? 2000 : false,
  });

  const { data: resultsData, isLoading: resultsLoading } = useQuery({
    queryKey: ['benchmarkResults', jobId],
    queryFn: () => apiService.getBenchmarkResults(jobId),
    enabled: statusData?.status === 'completed',
  });

  const isLoading = handleRun.isPending || statusLoading || resultsLoading;
  
  // Format dynamically based on algorithm counts natively from chart_data keys assuming Array maps.
  const chartData = resultsData?.chart_data?.metrics ? resultsData.chart_data.labels.map((lbl, idx) => ({
    name: lbl,
    score: resultsData.chart_data.metrics[idx]
  })) : [];

  return (
    <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
      <div className="flex justify-between items-end border-b pb-5 mb-8 border-gray-200">
         <div>
            <h3 className="text-3xl font-extrabold text-gray-900 flex items-center gap-3">
              <TrendingUp className="text-indigo-600 w-8 h-8"/> AutoML Benchmarks
            </h3>
            <p className="mt-2 text-sm text-gray-500">Run multiple parallel model training algorithms simultaneously.</p>
         </div>
         <button 
           onClick={() => handleRun.mutate()}
           disabled={isLoading}
           className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-400 gap-2"
         >
           {isLoading ? <Loader2 className="animate-spin w-4 h-4"/> : <Play className="w-4 h-4"/>} 
           Run Benchmark Ecosystem
         </button>
      </div>

      <div className="grid grid-cols-1 gap-6 mb-8 mt-4">
        {isLoading && (
          <div className="p-8 bg-indigo-50 border-2 border-indigo-200 rounded-lg text-center shadow-inner">
             <GaugeCircle className="w-16 h-16 text-indigo-400 mx-auto animate-pulse mb-4"/>
             <h4 className="text-indigo-800 font-bold text-lg mb-2">Executing Algorithm Stress Tests</h4>
             <div className="max-w-md mx-auto bg-gray-200 rounded-full h-2.5 mb-4">
               <div className="bg-indigo-600 h-2.5 rounded-full transition-all duration-500" style={{ width: `${statusData?.progress || 10}%` }}></div>
             </div>
             <p className="text-sm font-semibold text-indigo-600">Testing structural pipelines concurrently ({statusData?.status || 'starting'}...)</p>
          </div>
        )}

        {resultsData && !isLoading && (
          <>
            <div className="bg-white p-6 shadow-sm rounded-xl border border-gray-100">
              <h4 className="flex items-center text-lg font-bold text-gray-800 mb-6">Execution Accuracy Trajectories</h4>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="score" stroke="#4f46e5" strokeWidth={3} activeDot={{ r: 8 }} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </>
        )}
        
        {!resultsData && !isLoading && (
           <div className="flex flex-col items-center justify-center p-12 bg-gray-50 border-2 border-dashed border-gray-300 rounded-xl text-gray-500">
             <FilePlus className="w-12 h-12 text-gray-400 mb-4"/>
             <p className="font-semibold text-lg text-gray-700">No active benchmarks evaluated.</p>
             <p className="text-sm">Kick off a test using the execution button.</p>
           </div>
        )}
      </div>
    </div>
  );
};
export default BenchmarkPage;

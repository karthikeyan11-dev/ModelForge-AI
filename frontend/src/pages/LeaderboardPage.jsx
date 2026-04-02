import { useQuery } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { Trophy, Clock, Target, AlertTriangle, Loader2 } from 'lucide-react';
import React, { useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const LeaderboardPage = () => {
  const { data: leaderboardData, isLoading, isError, error } = useQuery({
    queryKey: ['leaderboard'],
    queryFn: () => apiService.getLeaderboard(), // Adjust to local state / mlops results accordingly securely
  });

  const [sortBy, setSortBy] = useState('cv_mean'); // State management cleanly extracted

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen">
        <Loader2 className="w-12 h-12 text-indigo-600 animate-spin" />
        <h2 className="mt-4 text-xl tracking-tight text-gray-700">Fetching AutoML Leaderboards...</h2>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen text-red-500">
        <AlertTriangle className="w-12 h-12 mb-4" />
        <h2 className="text-xl font-bold">Failed to load leaderboard</h2>
        <p className="opacity-80 mt-2">{error?.message || "Verify your API configurations."}</p>
      </div>
    );
  }

  // Handle Mock/Actual data securely 
  const processedData = leaderboardData || [];
  const chartData = processedData.slice(0, 5).map(item => ({
    name: item.algorithm,
    score: Number(item?.metrics?.cv_mean || 0).toFixed(4),
    time: Number(item?.metrics?.training_time || 0).toFixed(2),
  }));

  return (
    <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
      <div className="mb-8 border-b border-gray-200 pb-5">
        <h3 className="text-3xl font-extrabold text-gray-900 flex items-center gap-3">
          <Trophy className="text-amber-500 w-8 h-8" /> 
          SaaS Benchmark Ecosystem
        </h3>
        <p className="mt-2 max-w-4xl text-sm text-gray-500">
          Evaluations gathered across training paradigms. Compare model stability, duration, and native accuracies natively decoupled below.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-10">
        {/* Metric Tracker Line Chart */}
        <div className="bg-white p-6 shadow-sm rounded-xl border border-gray-100">
          <h4 className="text-lg font-bold text-gray-800 mb-6 flex items-center">
            <Target className="w-5 h-5 mr-2 text-indigo-500"/> Model Architecture Scores
          </h4>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis domain={['auto', 'auto']} />
                <Tooltip />
                <Legend />
                <Bar dataKey="score" fill="#6366f1" name="Target Score (Accuracy/RMSE)" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Time Tracking Comparison */}
        <div className="bg-white p-6 shadow-sm rounded-xl border border-gray-100">
          <h4 className="text-lg font-bold text-gray-800 mb-6 flex items-center">
            <Clock className="w-5 h-5 mr-2 text-rose-500"/> Compute Training Overheads (Seconds)
          </h4>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                <XAxis type="number" />
                <YAxis dataKey="name" type="category" width={100} tick={{ fontSize: 12 }}/>
                <Tooltip />
                <Legend />
                <Bar dataKey="time" fill="#f43f5e" name="Execution Duration" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="bg-white shadow-md rounded-xl overflow-hidden border border-gray-100">
        <div className="px-6 py-5 border-b border-gray-100 bg-gray-50/50 flex justify-between items-center">
           <h4 className="text-lg font-bold text-gray-800">Ranking Board</h4>
           <select 
             className="text-sm border-gray-300 rounded-lg bg-white shadow-sm focus:ring-indigo-500 focus:border-indigo-500 px-3 py-2"
             value={sortBy}
             onChange={(e) => setSortBy(e.target.value)}
           >
             <option value="cv_mean">Rank By Score</option>
             <option value="training_time">Rank By Compute Time</option>
           </select>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wider font-semibold">
              <tr>
                <th className="px-6 py-4 text-left font-medium">Rank</th>
                <th className="px-6 py-4 text-left font-medium">Algorithm Profile</th>
                <th className="px-6 py-4 text-left font-medium">Validation Score (CV)</th>
                <th className="px-6 py-4 text-left font-medium">Validation Std</th>
                <th className="px-6 py-4 text-left font-medium">Latency (s)</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {[...processedData].sort((a,b) => {
                 if(sortBy === 'cv_mean') return Number(b?.metrics?.cv_mean || 0) - Number(a?.metrics?.cv_mean || 0);
                 return Number(a?.metrics?.training_time || 0) - Number(b?.metrics?.training_time || 0);
              }).map((row, idx) => (
                <tr key={idx} className="hover:bg-gray-50/50 transition-colors">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold ${idx === 0 ? 'bg-amber-100 text-amber-800' : 'bg-gray-100 text-gray-800'}`}>
                      #{idx + 1}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{row.algorithm.replace(/_/g, ' ').toUpperCase()}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono font-medium">{Number(row?.metrics?.cv_mean || 0).toFixed(4)}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-400">±{Number(row?.metrics?.cv_std || 0).toFixed(4)}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{Number(row?.metrics?.training_time || 0).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default LeaderboardPage;

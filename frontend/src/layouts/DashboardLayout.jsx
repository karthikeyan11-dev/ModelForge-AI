import React from 'react';
import { NavLink, Outlet, Link } from 'react-router-dom';
import { LayoutDashboard, TrendingUp, Cpu, UploadCloud, LogOut, Wand2, Trophy, Settings, Beaker } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import AnimatedBackground from '../components/AnimatedBackground';
import { motion } from 'framer-motion';

const DashboardLayout = () => {
  const { logout, user } = useAuth();
  const navigation = [
    { name: 'Data Ingestion', href: '/upload', icon: UploadCloud },
    { name: 'Preprocess Data', href: '/preprocess', icon: Wand2 },
    { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard },
    { name: 'Model Leaderboard', href: '/leaderboard', icon: Trophy },
    { name: 'AutoML Benchmarks', href: '/benchmark', icon: TrendingUp },
    { name: 'Explainability Insight', href: '/explain', icon: Cpu },
  ];

  return (
    <div className="flex min-h-screen bg-slate-50 relative overflow-hidden">
      <AnimatedBackground />

      {/* Sidebar - Hardened with Styling Only */}
      <aside className="fixed inset-y-0 flex-col w-64 border-r border-purple-100 bg-white/80 backdrop-blur-xl z-20 shadow-xl shadow-purple-500/5">
        <div className="flex items-center h-20 flex-shrink-0 px-6 bg-gradient-to-br from-purple-700 to-indigo-600 shadow-lg">
          <div className="p-2 bg-white/20 rounded-xl mr-3">
             <Beaker className="text-white w-5 h-5" />
          </div>
          <h1 className="text-lg font-black text-white tracking-tighter uppercase italic">AutoML AI</h1>
        </div>

        <div className="flex-1 flex flex-col overflow-y-auto w-full pt-8 pb-4">
          <nav className="flex-1 px-4 space-y-2">
            {navigation.map((item) => (
              <NavLink
                key={item.name}
                to={item.href}
                className={({ isActive }) =>
                  `group flex items-center px-4 py-3 text-sm font-black rounded-2xl transition-all ${
                    isActive
                      ? 'bg-purple-600 text-white shadow-lg shadow-purple-500/20'
                      : 'text-slate-400 hover:bg-purple-50 hover:text-purple-700'
                  }`
                }
              >
                {({ isActive }) => (
                  <>
                    <item.icon
                      className={`mr-3 h-5 w-5 flex-shrink-0 transition-colors ${
                        isActive ? 'text-white' : 'text-slate-300 group-hover:text-purple-600'
                      }`}
                      aria-hidden="true"
                    />
                    {item.name}
                  </>
                )}
              </NavLink>
            ))}
          </nav>

          {/* Sidebar Footer */}
          <div className="p-4 mt-auto">
             <div className="bg-gradient-to-br from-purple-50 to-indigo-50 rounded-2xl p-4 border border-purple-100/50">
                <p className="text-[10px] font-black text-purple-700 uppercase tracking-widest mb-1">Status</p>
                <div className="flex items-center space-x-2">
                   <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></div>
                   <span className="text-xs font-bold text-slate-600">Engine Ready</span>
                </div>
             </div>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 ml-64 min-h-screen relative z-10 flex flex-col">
        {/* Header - Hardened with Styling Only */}
        <header className="bg-white/60 backdrop-blur-md border-b border-purple-100 sticky top-0 z-10">
          <div className="max-w-7xl mx-auto py-5 px-6 lg:px-8 flex items-center justify-between">
             <div className="flex items-center space-x-3">
                <div className="w-1.5 h-6 bg-purple-600 rounded-full"></div>
                <h2 className="text-xl font-black text-slate-900 tracking-tight leading-tight">Pipeline Control Center</h2>
             </div>
             
             <div className="flex items-center space-x-6">
                <button
                  onClick={logout}
                  className="flex items-center text-xs font-black text-slate-400 hover:text-red-500 transition-colors bg-white/50 px-4 py-2 rounded-xl border border-slate-100 hover:border-red-100"
                >
                  <LogOut className="h-4 w-4 mr-2" />
                  LOGOUT
                </button>
                <div className="flex items-center space-x-3 bg-white/80 p-1.5 pr-4 rounded-2xl border border-white shadow-sm transition-transform hover:scale-105 cursor-pointer">
                  <div className="h-8 w-8 rounded-xl bg-gradient-to-br from-purple-600 to-indigo-600 text-white flex items-center justify-center font-black text-xs shadow-lg shadow-purple-500/20">
                    {user?.email?.substring(0, 2).toUpperCase() || 'AI'}
                  </div>
                  <span className="text-xs font-black text-slate-500 tracking-tight truncate max-w-[120px]">
                    {user?.email || 'Administrator'}
                  </span>
                </div>
             </div>
          </div>
        </header>

        {/* Content Section - Hardened with Styling Only */}
        <div className="flex-1 p-8">
          <motion.div 
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="h-full"
          >
            <Outlet />
          </motion.div>
        </div>
      </main>
    </div>
  );
};

export default DashboardLayout;

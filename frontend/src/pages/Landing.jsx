import React from 'react';
import { Link, Navigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import GlassCard from '../components/GlassCard';
import AnimatedBackground from '../components/AnimatedBackground';
import Navbar from '../components/Navbar';
import { motion } from 'framer-motion';
import { 
  ArrowRight, CheckCircle2, Zap, BarChart3, Database, 
  Cpu, Layers, ShieldCheck, PieChart 
} from 'lucide-react';

const Landing = () => {
  const { user } = useAuth();

  // AUTH-AWARE ROUTING: Redirect authenticated users to the dashboard
  if (user) {
    return <Navigate to="/upload" replace />;
  }

  const features = [
    { title: "Intelligent Data Cleaning", icon: Database, desc: "Auto-detect missing values, encoding, and scaling needs." },
    { title: "Automated Feature Engineering", icon: Zap, desc: "Synthesize high-impact features with AI-driven insights." },
    { title: "Outlier Detection & Handling", icon: ShieldCheck, desc: "Protect model integrity by neutralizing data anomalies." },
    { title: "AutoML Benchmarking", icon: Cpu, desc: "Parallel training of LR, RF, XGBoost, and more." },
    { title: "MLflow Experiment Tracking", icon: BarChart3, desc: "Deep observability for every hyperparameter and run." },
    { title: "Real-time Leaderboard", icon: PieChart, desc: "Instant ranking of candidate models by primary metrics." },
  ];

  const systemFlow = ["Upload", "Clean", "Train", "Compare", "Deploy"];

  return (
    <div className="min-h-screen">
      <Navbar />
      <AnimatedBackground />

      {/* Hero Section */}
      <main className="relative pt-32 pb-20 px-6 overflow-hidden">
        <div className="max-w-7xl mx-auto flex flex-col items-center text-center">
          
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8 }}
            className="w-full max-w-4xl"
          >
            <GlassCard className="p-12 mb-16 shadow-2xl ring-1 ring-white/50" hover={false}>
              <div className="inline-flex items-center space-x-2 px-4 py-1.5 bg-purple-100 rounded-full mb-8">
                <Layers className="w-4 h-4 text-purple-700" />
                <span className="text-xs font-black uppercase tracking-widest text-purple-700">Next Gen MLOps Platform</span>
              </div>
              
              <h1 className="text-6xl md:text-7xl font-black tracking-tighter text-slate-900 leading-[0.9] mb-6">
                AI Data Cleaning <br />
                <span className="bg-clip-text text-transparent bg-gradient-to-r from-purple-700 via-indigo-600 to-purple-800">
                  + AutoML Platform
                </span>
              </h1>
              
              <p className="text-xl md:text-2xl font-bold text-slate-500 max-w-2xl mx-auto mb-10 leading-snug">
                From raw data to production-ready models — <br />
                fully automated with zero-leakage pipelines.
              </p>

              <div className="flex flex-col sm:flex-row items-center justify-center space-y-4 sm:space-y-0 sm:space-x-4">
                <Link 
                  to="/register" 
                  className="w-full sm:w-auto px-8 py-5 bg-slate-900 text-white rounded-3xl font-black text-lg transition-all hover:bg-purple-700 hover:-translate-y-1 shadow-xl shadow-purple-500/20 active:scale-95"
                >
                  Get Started Free
                </Link>
                <div className="flex items-center space-x-2 px-6 py-4 bg-white/50 rounded-3xl text-sm font-bold border border-white/50">
                   <CheckCircle2 className="w-4 h-4 text-emerald-500" />
                   <span>No Credit Card Required</span>
                </div>
              </div>
            </GlassCard>
          </motion.div>

          {/* System Flow Grid */}
          <div className="grid grid-cols-5 gap-4 max-w-3xl mx-auto mb-20">
            {systemFlow.map((step, i) => (
              <div key={i} className="flex flex-col items-center">
                <div className="w-12 h-12 rounded-2xl bg-white/40 flex items-center justify-center text-sm font-black border border-white focus:ring-4 focus:ring-purple-500/20 shadow-sm transition-all hover:bg-white/80">
                  {i + 1}
                </div>
                <span className="mt-2 text-[10px] font-black uppercase tracking-widest text-slate-500">{step}</span>
              </div>
            ))}
          </div>

          {/* Features Grid */}
          <div className="max-w-7xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {features.map((feature, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 + i * 0.1 }}
              >
                <GlassCard className="p-8 h-full text-left group">
                  <div className="p-3 bg-purple-50 rounded-2xl w-fit mb-6 transition-transform group-hover:scale-110">
                    <feature.icon className="w-6 h-6 text-purple-700" />
                  </div>
                  <h3 className="text-lg font-black text-slate-900 tracking-tight mb-3">
                    {feature.title}
                  </h3>
                  <p className="text-slate-500 text-sm font-bold leading-relaxed">
                    {feature.desc}
                  </p>
                </GlassCard>
              </motion.div>
            ))}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="py-12 border-t border-slate-100 bg-white/20 backdrop-blur-sm">
         <div className="max-w-7xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center space-y-4 md:space-y-0">
           <span className="text-sm font-bold text-slate-400 uppercase tracking-widest">© 2026 AutoML Platform</span>
           <div className="flex items-center space-x-6 text-sm font-bold text-slate-400">
             <a href="#" className="hover:text-purple-600 transition-colors">Privacy</a>
             <a href="#" className="hover:text-purple-600 transition-colors">Documentation</a>
             <a href="#" className="hover:text-purple-600 transition-colors">Support</a>
           </div>
         </div>
      </footer>
    </div>
  );
};

export default Landing;

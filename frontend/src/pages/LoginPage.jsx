import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import axios from 'axios';
import { Mail, Lock, LogIn, AlertCircle, Loader2, ArrowRight, Cpu } from 'lucide-react';
import GlassCard from '../components/GlassCard';
import AnimatedBackground from '../components/AnimatedBackground';
import Navbar from '../components/Navbar';
import { motion } from 'framer-motion';

const LoginPage = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [fieldErrors, setFieldErrors] = useState({});
  const [isLoading, setIsLoading] = useState(false);
  
  const navigate = useNavigate();
  const { login } = useAuth();

  // PRESERVE: Existing login logic
  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    setFieldErrors({});
    setIsLoading(true);
    
    try {
      const response = await axios.post(`${import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v2'}/auth/login`, {
        email,
        password
      });
      
      const { access_token } = response.data;
      login(access_token);
      navigate('/upload'); // Redirect to dashboard entry point
    } catch (err) {
      console.error(err);
      const detail = err.response?.data?.detail;
      
      if (Array.isArray(detail)) {
        const errors = {};
        detail.forEach(errItem => {
          const field = errItem.loc[errItem.loc.length - 1];
          errors[field] = errItem.msg;
        });
        setFieldErrors(errors);
        setError('Please check the fields below');
      } else {
        setError(detail || 'Invalid email or password. Please try again.');
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-6 bg-slate-50 relative overflow-hidden">
      <Navbar />
      <AnimatedBackground />

      <motion.div 
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="w-full max-w-md"
      >
        <GlassCard className="p-10 shadow-2xl border-white/50 backdrop-blur-2xl">
          <div className="text-center mb-10">
            <div className="inline-flex items-center justify-center p-4 bg-purple-100 rounded-3xl mb-6 shadow-sm ring-8 ring-purple-50/50">
              <Cpu size={32} className="text-purple-600" />
            </div>
            <h1 className="text-3xl font-black text-slate-900 tracking-tight">Welcome Back</h1>
            <p className="mt-2 text-slate-400 font-bold uppercase tracking-widest text-[10px]">Secure Login Portal</p>
          </div>

          {error && !Object.keys(fieldErrors).length && (
            <motion.div 
              initial={{ x: -10, opacity: 0 }}
              animate={{ x: 0, opacity: 1 }}
              className="mb-8 p-4 bg-red-50 border-l-4 border-red-500 rounded-2xl flex items-start text-red-700 ring-1 ring-red-100"
            >
              <AlertCircle className="w-5 h-5 mr-3 flex-shrink-0 mt-0.5" />
              <p className="text-xs font-bold tracking-tight">{error}</p>
            </motion.div>
          )}

          <form onSubmit={handleLogin} className="space-y-6">
            <div className="space-y-2">
              <label className="block text-[10px] font-black text-slate-400 tracking-widest uppercase ml-1" htmlFor="email">
                Email Address
              </label>
              <div className={`relative group ${fieldErrors.email ? 'ring-2 ring-red-500/20' : ''}`}>
                <span className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${fieldErrors.email ? 'text-red-400' : 'text-slate-400 group-focus-within:text-purple-600'}`}>
                  <Mail size={18} />
                </span>
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className={`w-full pl-12 pr-4 py-4 bg-white/50 border ${fieldErrors.email ? 'border-red-300' : 'border-slate-200'} rounded-2xl focus:ring-4 focus:ring-purple-500/10 focus:border-purple-600 outline-none transition-all placeholder:text-slate-300 font-bold text-sm`}
                  placeholder="you@email.com"
                  required
                />
              </div>
              {fieldErrors.email && <p className="text-[10px] font-bold text-red-500 mt-1 ml-1">{fieldErrors.email}</p>}
            </div>

            <div className="space-y-2">
              <label className="block text-[10px] font-black text-slate-400 tracking-widest uppercase ml-1" htmlFor="password">
                Password
              </label>
              <div className={`relative group ${fieldErrors.password ? 'ring-2 ring-red-500/20' : ''}`}>
                <span className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${fieldErrors.password ? 'text-red-400' : 'text-slate-400 group-focus-within:text-purple-600'}`}>
                  <Lock size={18} />
                </span>
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className={`w-full pl-12 pr-4 py-4 bg-white/50 border ${fieldErrors.password ? 'border-red-300' : 'border-slate-200'} rounded-2xl focus:ring-4 focus:ring-purple-500/10 focus:border-purple-600 outline-none transition-all placeholder:text-slate-300 font-bold text-sm`}
                  placeholder="••••••••"
                  required
                />
              </div>
              {fieldErrors.password && <p className="text-[10px] font-bold text-red-500 mt-1 ml-1">{fieldErrors.password}</p>}
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className={`w-full py-4 bg-purple-600 hover:bg-slate-900 text-white font-black rounded-2xl shadow-xl shadow-purple-500/20 transition-all flex items-center justify-center group h-14 ${isLoading ? 'opacity-70 cursor-not-allowed' : 'hover:-translate-y-0.5'}`}
            >
              {isLoading ? (
                <>
                  <Loader2 className="animate-spin mr-3 h-5 w-5" />
                  Authenticating...
                </>
              ) : (
                <>
                  Login to Platform
                  <ArrowRight size={20} className="ml-3 group-hover:translate-x-1.5 transition-transform" />
                </>
              )}
            </button>
          </form>

          <div className="mt-8 pt-8 border-t border-slate-100 text-center text-xs font-bold text-slate-400">
            New user?{' '}
            <Link to="/register" className="text-purple-600 hover:text-slate-900 transition-colors uppercase tracking-widest ml-1 decoration-2 underline underline-offset-4 decoration-purple-100">
              Create Account
            </Link>
          </div>
        </GlassCard>
      </motion.div>
    </div>
  );
};

export default LoginPage;

import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Mail, Lock, UserPlus, AlertCircle, Loader2, CheckCircle, ArrowRight, Cpu } from 'lucide-react';
import GlassCard from '../components/GlassCard';
import AnimatedBackground from '../components/AnimatedBackground';
import Navbar from '../components/Navbar';
import { motion, AnimatePresence } from 'framer-motion';

const RegisterPage = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState('');
  const [fieldErrors, setFieldErrors] = useState({});
  const [isLoading, setIsLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  
  const navigate = useNavigate();

  // PRESERVE: Existing registration logic
  const handleRegister = async (e) => {
    e.preventDefault();
    setError('');
    setFieldErrors({});
    
    if (password !== confirmPassword) {
      setFieldErrors({ confirm: 'Passwords do not match' });
      return;
    }
    
    setIsLoading(true);
    
    try {
      await axios.post(`${import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v2'}/auth/register`, {
        email,
        password
      });
      
      setSuccess(true);
      setTimeout(() => navigate('/login'), 2500);
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
        setError('Please fix the errors below');
      } else {
        setError(detail || 'An error occurred during registration');
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-6 bg-slate-50 relative overflow-hidden">
      <Navbar />
      <AnimatedBackground />

      <AnimatePresence mode="wait">
        {success ? (
          <motion.div
            key="success"
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 1.1 }}
            className="w-full max-w-md"
          >
            <GlassCard className="p-12 text-center border-emerald-100 bg-emerald-50/30">
               <div className="w-24 h-24 bg-emerald-100 rounded-full flex items-center justify-center mx-auto mb-8 shadow-inner ring-8 ring-emerald-50">
                  <CheckCircle size={48} className="text-emerald-600" />
               </div>
               <h1 className="text-3xl font-black text-slate-900 mb-4">You're All Set! ✅</h1>
               <p className="text-slate-500 font-bold mb-8 px-4 leading-relaxed">
                  Your account has been successfully created. We're redirecting you to login...
               </p>
               <div className="flex items-center justify-center space-x-2 text-emerald-600 font-black">
                  <span className="w-2 h-2 bg-emerald-600 rounded-full animate-ping"></span>
                  <span className="tracking-widest uppercase text-xs">Redirecting</span>
               </div>
            </GlassCard>
          </motion.div>
        ) : (
          <motion.div 
            key="form"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="w-full max-w-md"
          >
            <GlassCard className="p-10 shadow-2xl border-white/50 backdrop-blur-2xl">
              <div className="text-center mb-10">
                <div className="inline-flex items-center justify-center p-4 bg-indigo-100 rounded-3xl mb-6 shadow-sm ring-8 ring-indigo-50/50">
                  <UserPlus size={32} className="text-indigo-600" />
                </div>
                <h1 className="text-3xl font-black text-slate-900 tracking-tight">Create Account</h1>
                <p className="mt-2 text-slate-400 font-bold uppercase tracking-widest text-[10px]">Start your AutoML journey</p>
              </div>

              {error && !Object.keys(fieldErrors).length && (
                <div className="mb-6 p-4 bg-red-50 border-l-4 border-red-500 rounded-2xl flex items-start text-red-700 ring-1 ring-red-100">
                  <AlertCircle className="w-5 h-5 mr-3 flex-shrink-0 mt-0.5" />
                  <p className="text-xs font-bold tracking-tight">{error}</p>
                </div>
              )}

              <form onSubmit={handleRegister} className="space-y-5">
                <div className="space-y-2">
                  <label className="block text-[10px] font-black text-slate-400 tracking-widest uppercase ml-1" htmlFor="email">
                    Email Address
                  </label>
                  <div className={`relative group ${fieldErrors.email ? 'ring-2 ring-red-500/20' : ''}`}>
                    <span className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${fieldErrors.email ? 'text-red-400' : 'text-slate-400 group-focus-within:text-indigo-600'}`}>
                      <Mail size={18} />
                    </span>
                    <input
                      id="email"
                      type="email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      className={`w-full pl-12 pr-4 py-4 bg-white/50 border ${fieldErrors.email ? 'border-red-300' : 'border-slate-200'} rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-600 outline-none transition-all placeholder:text-slate-300 font-bold text-sm`}
                      placeholder="alex@company.com"
                      required
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <label className="block text-[10px] font-black text-slate-400 tracking-widest uppercase ml-1" htmlFor="password">
                    Password
                  </label>
                  <div className={`relative group ${fieldErrors.password ? 'ring-2 ring-red-500/20' : ''}`}>
                    <span className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${fieldErrors.password ? 'text-red-400' : 'text-slate-400 group-focus-within:text-indigo-600'}`}>
                      <Lock size={18} />
                    </span>
                    <input
                      id="password"
                      type="password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className={`w-full pl-12 pr-4 py-4 bg-white/50 border ${fieldErrors.password ? 'border-red-300' : 'border-slate-200'} rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-600 outline-none transition-all placeholder:text-slate-300 font-bold text-sm`}
                      placeholder="••••••••"
                      required
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <label className="block text-[10px] font-black text-slate-400 tracking-widest uppercase ml-1" htmlFor="confirm">
                    Confirm Password
                  </label>
                  <div className={`relative group ${fieldErrors.confirm ? 'ring-2 ring-red-500/20' : ''}`}>
                    <span className={`absolute left-4 top-1/2 -translate-y-1/2 transition-colors ${fieldErrors.confirm ? 'text-red-400' : 'text-slate-400 group-focus-within:text-indigo-600'}`}>
                      <Lock size={18} />
                    </span>
                    <input
                      id="confirm"
                      type="password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      className={`w-full pl-12 pr-4 py-4 bg-white/50 border ${fieldErrors.confirm ? 'border-red-300' : 'border-slate-200'} rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-600 outline-none transition-all placeholder:text-slate-300 font-bold text-sm`}
                      placeholder="Repeat password"
                      required
                    />
                  </div>
                  {fieldErrors.confirm && <p className="text-[10px] font-bold text-red-500 mt-1 ml-1">{fieldErrors.confirm}</p>}
                </div>

                <button
                  type="submit"
                  disabled={isLoading}
                  className={`w-full py-4 bg-indigo-600 hover:bg-slate-900 text-white font-black rounded-2xl shadow-xl shadow-indigo-500/20 transition-all flex items-center justify-center group h-14 ${isLoading ? 'opacity-70 cursor-not-allowed' : 'hover:-translate-y-0.5'}`}
                >
                  {isLoading ? (
                    <Loader2 className="animate-spin mr-3 h-5 w-5" />
                  ) : (
                    <>
                      Register Account
                      <ArrowRight size={20} className="ml-3 group-hover:translate-x-1.5 transition-transform" />
                    </>
                  )}
                </button>
              </form>

              <div className="mt-8 pt-8 border-t border-slate-100 text-center text-xs font-bold text-slate-400">
                Already registered?{' '}
                <Link to="/login" className="text-indigo-600 hover:text-slate-900 transition-colors uppercase tracking-widest ml-1 decoration-2 underline underline-offset-4 decoration-indigo-200">
                  Sign In
                </Link>
              </div>
            </GlassCard>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

export default RegisterPage;

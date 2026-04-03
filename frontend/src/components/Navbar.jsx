import React from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { LogIn, UserPlus, LayoutDashboard, LogOut, Cpu } from 'lucide-react';
import { motion } from 'framer-motion';

const Navbar = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const isAuthPage = ['/login', '/register'].includes(location.pathname);

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 px-6 py-4">
      <div className="max-w-7xl mx-auto flex items-center justify-between bg-white/30 backdrop-blur-md border border-white/20 rounded-3xl px-6 py-2 shadow-sm ring-1 ring-black/5">
        
        {/* Logo */}
        <Link to="/" className="flex items-center space-x-2 group">
          <div className="p-2 bg-purple-600 rounded-2xl shadow-lg shadow-purple-500/20 transition-transform group-hover:scale-110">
            <Cpu className="text-white w-5 h-5" />
          </div>
          <span className="text-lg font-black tracking-tighter bg-clip-text text-transparent bg-gradient-to-r from-purple-700 to-indigo-700">
            Prism AI
          </span>
        </Link>

        {/* Navigation Links */}
        <div className="flex items-center space-x-2">
          {!user ? (
            <>
              {location.pathname !== '/login' && (
                <Link to="/login" className="flex items-center space-x-2 px-4 py-2 text-sm font-bold text-purple-700 hover:bg-purple-50 rounded-2xl transition-all">
                  <LogIn className="w-4 h-4" />
                  <span>Login</span>
                </Link>
              )}
              {location.pathname !== '/register' && (
                <Link to="/register" className="flex items-center space-x-2 px-5 py-2.5 text-sm font-black bg-purple-600 text-white rounded-2xl shadow-lg shadow-purple-500/20 hover:bg-slate-900 transition-all hover:-translate-y-0.5">
                  <UserPlus className="w-4 h-4" />
                  <span>Register</span>
                </Link>
              )}
            </>
          ) : (
            <>
              <Link to="/upload" className="flex items-center space-x-2 px-4 py-2 text-sm font-bold text-indigo-700 hover:bg-indigo-50 rounded-2xl transition-all">
                <LayoutDashboard className="w-4 h-4" />
                <span>Dashboard</span>
              </Link>
              <button 
                onClick={handleLogout}
                className="flex items-center space-x-2 px-4 py-2 text-sm font-bold text-slate-500 hover:text-red-500 hover:bg-red-50 rounded-2xl transition-all"
              >
                <LogOut className="w-4 h-4" />
                <span>Logout</span>
              </button>
            </>
          )}
        </div>
      </div>
    </nav>
  );
};

export default Navbar;

import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useMutation } from '@tanstack/react-query';
import { Bot } from 'lucide-react';
import { authApi } from '../services/api';
import { useAuthStore } from '../store/authStore';

export const LoginPage = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();
  const { setAuth } = useAuthStore();

  const loginMutation = useMutation({
    mutationFn: () => authApi.login(username, password),
    onSuccess: (data) => {
      setAuth(data.user, data.access_token);
      navigate('/');
    },
    onError: (err: any) => {
      setError(err.response?.data?.message || 'Login failed. Please try again.');
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    loginMutation.mutate();
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <div className="w-full max-w-md p-8 glass-panel rounded-3xl animate-in fade-in slide-in-from-bottom-4 duration-500">
        <div className="flex flex-col items-center mb-8">
          <div className="w-20 h-20 bg-indigo-600 rounded-2xl flex items-center justify-center mb-6 shadow-lg shadow-indigo-600/30 transform hover:scale-110 transition-transform duration-300">
            <Bot size={40} className="text-white" />
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Welcome Back</h1>
          <p className="text-slate-300 mt-2 font-medium">Sign in to BuildTL</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {error && (
            <div className="p-4 bg-red-50/50 border border-red-200/50 backdrop-blur-sm rounded-xl text-red-600 text-sm font-medium animate-in fade-in slide-in-from-top-2">
              {error}
            </div>
          )}

          <div>
            <label htmlFor="username" className="block text-sm font-semibold text-slate-300 mb-2">
              Username
            </label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-3 glass-input rounded-xl outline-none text-white placeholder:text-slate-400"
              required
            />
          </div>

          <div>
            <label htmlFor="password" className="block text-sm font-semibold text-slate-300 mb-2">
              Password
            </label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-3 glass-input rounded-xl outline-none text-white placeholder:text-slate-400"
              required
            />
          </div>

          <button
            type="submit"
            disabled={loginMutation.isPending}
            className="w-full py-3.5 bg-indigo-600 text-white font-semibold rounded-xl hover:bg-indigo-500 transition-all shadow-lg hover:shadow-indigo-500/30 hover:-translate-y-0.5 disabled:opacity-70 disabled:hover:translate-y-0"
          >
            {loginMutation.isPending ? 'Signing in...' : 'Sign In'}
          </button>

          <div className="text-center text-sm pt-2">
            <span className="text-slate-400">Don't have an account? </span>
            <Link to="/register" className="text-indigo-400 font-semibold hover:text-indigo-300 hover:underline">
              Create account
            </Link>
          </div>
        </form>
      </div>
    </div>
  );
};

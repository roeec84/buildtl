import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useMutation } from '@tanstack/react-query';
import { Bot } from 'lucide-react';
import { authApi } from '../services/api';

export const RegisterPage = () => {
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [organization, setOrganization] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const registerMutation = useMutation({
    mutationFn: () => authApi.register(username, email, password, organization || undefined),
    onSuccess: () => {
      navigate('/login', { state: { message: 'Registration successful! Please log in.' } });
    },
    onError: (err: any) => {
      setError(err.response?.data?.detail || 'Registration failed. Please try again.');
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    if (password.length < 6) {
      setError('Password must be at least 6 characters long');
      return;
    }

    registerMutation.mutate();
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <div className="w-full max-w-md p-8 glass-panel rounded-3xl animate-in fade-in slide-in-from-bottom-4 duration-500">
        <div className="flex flex-col items-center mb-8">
          <div className="w-20 h-20 bg-indigo-600 rounded-2xl flex items-center justify-center mb-6 shadow-lg shadow-indigo-600/30 transform hover:scale-110 transition-transform duration-300">
            <Bot size={40} className="text-white" />
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Create Account</h1>
          <p className="text-slate-300 mt-2 font-medium">Join BuildTL today</p>
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
            <label htmlFor="email" className="block text-sm font-semibold text-slate-300 mb-2">
              Email
            </label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full px-4 py-3 glass-input rounded-xl outline-none text-white placeholder:text-slate-400"
              required
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
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

            <div>
              <label htmlFor="confirmPassword" className="block text-sm font-semibold text-slate-300 mb-2">
                Confirm
              </label>
              <input
                id="confirmPassword"
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                className="w-full px-4 py-3 glass-input rounded-xl outline-none text-white placeholder:text-slate-400"
                required
              />
            </div>
          </div>

          <div>
            <label htmlFor="organization" className="block text-sm font-semibold text-slate-300 mb-2">
              Organization (optional)
            </label>
            <input
              id="organization"
              type="text"
              value={organization}
              onChange={(e) => setOrganization(e.target.value)}
              className="w-full px-4 py-3 glass-input rounded-xl outline-none text-white placeholder:text-slate-400"
            />
          </div>

          <button
            type="submit"
            disabled={registerMutation.isPending}
            className="w-full py-3.5 bg-indigo-600 text-white font-semibold rounded-xl hover:bg-indigo-500 transition-all shadow-lg hover:shadow-indigo-500/30 hover:-translate-y-0.5 disabled:opacity-70 disabled:hover:translate-y-0"
          >
            {registerMutation.isPending ? 'Creating account...' : 'Create Account'}
          </button>

          <div className="text-center text-sm pt-2">
            <span className="text-slate-400">Already have an account? </span>
            <Link to="/login" className="text-indigo-400 font-semibold hover:text-indigo-300 hover:underline">
              Sign in
            </Link>
          </div>
        </form>
      </div>
    </div>
  );
};

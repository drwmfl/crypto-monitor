import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    // ✅ 关键修复：允许你的域名访问
    allowedHosts: [
      'alphamonitorpro.com',
      'www.alphamonitorpro.com',
      'localhost' 
    ]
  }
})
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    // ✅ 关键配置：开启轮询，强制 Docker 监听 Windows 文件变化
    watch: {
      usePolling: true,
    },
    allowedHosts: [
      'alphamonitorpro.com',
      'www.alphamonitorpro.com',
      'localhost' 
    ]
  }
})
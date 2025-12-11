import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: true, // 监听所有 IP，Docker 必须
    port: 5173,
    watch: {
      usePolling: true,   // 🟢 关键：开启轮询模式，强制检查文件变化
      interval: 100       // 每 100ms 检查一次
    }
  }
})
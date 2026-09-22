// /health → 本机后端 /health（经 ngrok 隧道），详见 _proxy.ts
// 前端 api.health() 调用根路径 /health，由 vercel.json rewrite 到本函数。
import { proxy } from './_proxy'

export const config = { runtime: 'edge' }

export default function handler(req: Request): Promise<Response> {
  return proxy(req, '/health')
}

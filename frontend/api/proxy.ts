// /api/* → 本机后端（经 ngrok 隧道），详见 _proxy.ts
//
// 注意：零配置函数的动态路由（api/[...path].ts）在本项目（非 Next.js）的
// vercel.json 路由阶段不生效，因此用 rewrite 把 /api/* 统一转到本函数，
// 原始路径放在 ?path= 里重建。
import { proxy } from './_proxy'

export const config = { runtime: 'edge' }

export default function handler(req: Request): Promise<Response> {
  const url = new URL(req.url)
  const path = url.searchParams.get('path') ?? '/'
  // 去掉路由用的 path 参数，其余 query 原样透传
  const rest = new URLSearchParams(url.searchParams)
  rest.delete('path')
  const qs = rest.toString()
  return proxy(req, `/api${path}`, qs ? `?${qs}` : '')
}

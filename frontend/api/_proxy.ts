// 共享反向代理逻辑 — 把浏览器请求转发到本机后端（经 ngrok 固定域名隧道）。
//
// 为什么不用 vercel.json 静态 rewrite：ngrok 免费版会对"浏览器特征"的请求
// 返回拦截警告页（HTML/text-plain 而非后端响应），静态 rewrite 无法注入
// 请求头，而 Edge Function 可以统一注入 ngrok-skip-browser-warning 绕过头，
// 普通 fetch 与原生 EventSource（不能自定义请求头）都因此可用。
//
// 注意：Vercel Edge 在首个响应字节到达前不下发响应头，SSE 端点在上游
// 静默期（如实时行情关闭时）表现为"连接中"，有事件后立即推送——属预期行为。
//
// 后端地址默认指向 ngrok 固定域名，可在 Vercel 项目环境变量
// BACKEND_ORIGIN 中覆盖（换隧道/换服务器时不用改代码）。

export const BACKEND_ORIGIN =
  process.env.BACKEND_ORIGIN ?? 'https://uniflagellate-unconfoundingly-linn.ngrok-free.dev'

// 逐跳头不能透传；host 由 fetch 按目标 URL 重新设置
const HOP_BY_HOP = ['host', 'connection', 'content-length', 'transfer-encoding', 'keep-alive', 'upgrade']

export async function proxy(req: Request, targetPath: string, search?: string): Promise<Response> {
  const url = new URL(req.url)
  const headers = new Headers(req.headers)
  for (const h of HOP_BY_HOP) headers.delete(h)
  headers.set('ngrok-skip-browser-warning', 'true')

  const hasBody = req.method !== 'GET' && req.method !== 'HEAD'
  try {
    const upstream = await fetch(`${BACKEND_ORIGIN}${targetPath}${search ?? url.search}`, {
      method: req.method,
      headers,
      body: hasBody ? req.body : undefined,
      redirect: 'manual',
      // 转发流式 body 必须声明 duplex
      ...({ duplex: hasBody ? 'half' : undefined } as RequestInit),
    })
    const respHeaders = new Headers(upstream.headers)
    for (const h of HOP_BY_HOP) respHeaders.delete(h)
    return new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: respHeaders,
    })
  } catch (e) {
    return new Response(
      JSON.stringify({ detail: `后端隧道不可达: ${e instanceof Error ? e.message : e}` }),
      { status: 502, headers: { 'content-type': 'application/json' } },
    )
  }
}

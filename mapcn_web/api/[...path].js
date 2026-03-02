const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);
const RESPONSE_HEADERS_TO_STRIP = new Set(["content-encoding", "content-length"]);
const REQUEST_HEADERS_TO_STRIP = new Set(["host", "accept-encoding"]);

function getRequestBody(req) {
  if (req.method === "GET" || req.method === "HEAD") {
    return undefined;
  }
  if (req.body == null) {
    return undefined;
  }
  if (typeof req.body === "string" || req.body instanceof Buffer) {
    return req.body;
  }
  return JSON.stringify(req.body);
}

export default async function handler(req, res) {
  const backendOrigin = String(process.env.BACKEND_ORIGIN || "").trim();
  if (!backendOrigin) {
    res.status(500).json({
      error: "Missing BACKEND_ORIGIN environment variable",
    });
    return;
  }

  const pathParam = req.query.path;
  const upstreamPath = Array.isArray(pathParam) ? pathParam.join("/") : String(pathParam || "");
  const incomingUrl = new URL(req.url, "http://localhost");
  const upstreamUrl = new URL(`/api/${upstreamPath}`, backendOrigin);
  upstreamUrl.search = incomingUrl.search;

  const requestHeaders = {};
  for (const [name, value] of Object.entries(req.headers || {})) {
    const lower = name.toLowerCase();
    if (!value || HOP_BY_HOP_HEADERS.has(lower) || REQUEST_HEADERS_TO_STRIP.has(lower)) {
      continue;
    }
    requestHeaders[name] = value;
  }

  const upstreamResponse = await fetch(upstreamUrl, {
    method: req.method,
    headers: requestHeaders,
    body: getRequestBody(req),
    redirect: "manual",
  });

  for (const [name, value] of upstreamResponse.headers.entries()) {
    const lower = name.toLowerCase();
    if (HOP_BY_HOP_HEADERS.has(lower) || RESPONSE_HEADERS_TO_STRIP.has(lower)) {
      continue;
    }
    res.setHeader(name, value);
  }

  const body = Buffer.from(await upstreamResponse.arrayBuffer());
  res.status(upstreamResponse.status).send(body);
}

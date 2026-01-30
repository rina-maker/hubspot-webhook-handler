import crypto from "crypto";

export async function POST(request: Request) {
  const signature = request.headers.get("x-hubspot-signature-v3") ?? "";
  const timestamp = request.headers.get("x-hubspot-request-timestamp") ?? "";

  const now = Date.now();
  const ts = Number(timestamp);
  if (!ts || Math.abs(now - ts) > 5 * 60 * 1000) {
    return new Response("Invalid timestamp", { status: 401 });
  }

  const secret = process.env.HUBSPOT_CLIENT_SECRET;
  if (!secret) return new Response("Missing HUBSPOT_CLIENT_SECRET", { status: 500 });

  const rawBody = await request.text();

  const url = new URL(request.url);
  const requestUri = decodeURIComponent(`${url.pathname}${url.search}`);

  const sourceString = `${request.method}${requestUri}${rawBody}${timestamp}`;

  const computed = crypto
    .createHmac("sha256", secret)
    .update(sourceString, "utf8")
    .digest("base64");

  const a = Buffer.from(computed);
  const b = Buffer.from(signature);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return new Response("Invalid signature", { status: 401 });
  }

  return Response.json({ ok: true });
}

export async function GET() {
  return new Response("OK", { status: 200 });
}

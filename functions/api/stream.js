// /functions/api/stream.js
// Streams an R2 object by key with byte-range support for <video>.

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    if (!key) return new Response("Missing key", { status: 400 });

    const head = await env.R2_BUCKET.head(key);
    if (!head) return new Response("Not found", { status: 404 });

    const size = head.size;
    const etag = head.httpEtag || head.etag || "";
    const lastMod = head.uploaded?.toUTCString?.() || new Date().toUTCString();
    const ctype =
      head.httpMetadata?.contentType ||
      head.customMetadata?.contentType ||
      "application/octet-stream";

    const headers = new Headers({
      "Accept-Ranges": "bytes",
      "Cache-Control": "private, max-age=0, must-revalidate",
      ETag: etag,
      "Last-Modified": lastMod,
      "Content-Type": ctype,
    });

    const range = request.headers.get("Range");
    if (range && /^bytes=\d*-?\d*$/.test(range)) {
      const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
      let start = startStr ? parseInt(startStr, 10) : 0;
      let end = endStr ? parseInt(endStr, 10) : size - 1;
      if (Number.isNaN(start)) start = 0;
      if (Number.isNaN(end) || end >= size) end = size - 1;
      if (start > end) start = 0;

      const length = end - start + 1;
      const obj = await env.R2_BUCKET.get(key, { range: { offset: start, length } });
      if (!obj) return new Response("Not found", { status: 404 });

      headers.set("Content-Range", `bytes ${start}-${end}/${size}`);
      headers.set("Content-Length", String(length));
      return new Response(obj.body, { status: 206, headers });
    }

    const obj = await env.R2_BUCKET.get(key);
    if (!obj) return new Response("Not found", { status: 404 });
    headers.set("Content-Length", String(size));
    return new Response(obj.body, { status: 200, headers });
  } catch {
    return new Response("Server error", { status: 500 });
  }
};

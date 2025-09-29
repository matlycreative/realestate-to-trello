// /functions/api/stream.js
// Streams an object from R2 with Range support.
// If ?download=1 is present, add Content-Disposition: attachment.

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    if (!key) return new Response("Missing key", { status: 400 });

    const head = await env.R2_BUCKET.head(key);
    if (!head) return new Response("Not found", { status: 404 });

    const ctype = head.httpMetadata?.contentType || "application/octet-stream";
    const size  = head.size;

    // Parse Range (bytes=START-END)
    const range = request.headers.get("range");
    let respHeaders = new Headers({
      "Accept-Ranges": "bytes",
      "Content-Type": ctype,
      "Cache-Control": "no-store",
    });

    // Optional download
    if (url.searchParams.get("download")) {
      const filename = key.split("/").pop() || "video.mp4";
      respHeaders.set("Content-Disposition", `attachment; filename="${filename}"`);
    }

    if (range) {
      const m = range.match(/bytes=(\d+)-(\d+)?/);
      if (m) {
        const start = Number(m[1]);
        const end   = m[2] ? Number(m[2]) : size - 1;
        const length = end - start + 1;

        const obj = await env.R2_BUCKET.get(key, { range: { offset: start, length } });
        if (!obj) return new Response("Not found", { status: 404 });

        respHeaders.set("Content-Range", `bytes ${start}-${end}/${size}`);
        respHeaders.set("Content-Length", String(length));
        return new Response(obj.body, { status: 206, headers: respHeaders });
      }
    }

    // No Range → full object
    const obj = await env.R2_BUCKET.get(key);
    if (!obj) return new Response("Not found", { status: 404 });

    respHeaders.set("Content-Length", String(size));
    return new Response(obj.body, { status: 200, headers: respHeaders });
  } catch (e) {
    return new Response("Server error", { status: 500 });
  }
};

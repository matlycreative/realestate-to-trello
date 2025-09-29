// /functions/api/stream.js
// Streams a private R2 object with proper byte-range support.
// Usage: /api/stream?key=<r2_key>[&download=1]

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    const wantDownload = url.searchParams.get("download") === "1";
    if (!key) return new Response("Missing key", { status: 400 });

    // HEAD the object to get size + content-type
    const head = await env.R2_BUCKET.head(key);
    if (!head) return new Response("Not found", { status: 404 });

    const ct = head.httpMetadata?.contentType || "video/mp4";
    const totalSize = head.size;

    const rangeHeader = request.headers.get("Range");
    const baseHeaders = new Headers({
      "Content-Type": ct,
      "Accept-Ranges": "bytes",
      "Cache-Control": "private, max-age=0, no-store",
    });

    if (wantDownload) {
      const filename = key.split("/").pop() || "video.mp4";
      baseHeaders.set(
        "Content-Disposition",
        `attachment; filename="${filename.replace(/"/g, "")}"`
      );
    }

    // If the browser asked for a byte range, honor it
    if (rangeHeader) {
      const m = /^bytes=(\d+)-(\d+)?$/i.exec(rangeHeader);
      if (m) {
        const start = Number(m[1]);
        const end = m[2] ? Number(m[2]) : totalSize - 1;
        const length = end - start + 1;

        // Fetch the requested slice
        const obj = await env.R2_BUCKET.get(key, {
          range: { offset: start, length },
        });
        if (!obj) return new Response("Not found", { status: 404 });

        const h = new Headers(baseHeaders);
        h.set("Content-Range", `bytes ${start}-${end}/${totalSize}`);
        h.set("Content-Length", String(length));

        return new Response(obj.body, { status: 206, headers: h });
      }
      // If Range header is malformed, just fall through and send the full object
    }

    // No range → send full object
    const obj = await env.R2_BUCKET.get(key);
    if (!obj) return new Response("Not found", { status: 404 });

    const h = new Headers(baseHeaders);
    h.set("Content-Length", String(totalSize));
    return new Response(obj.body, { status: 200, headers: h });
  } catch (err) {
    return new Response("Server error", { status: 500 });
  }
};

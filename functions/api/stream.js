// /functions/api/stream.js
// Streams an object from R2 by key (query ?key=videos/<safe>__file.mp4)
export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    if (!key) return new Response("Missing key", { status: 400 });

    // Look up object metadata
    const head = await env.R2_BUCKET.head(key);
    if (!head) return new Response("Not found", { status: 404 });

    // --- Step 3: robust content-type detection ---
    const guessType = (k) => {
      const ext = k.split(".").pop()?.toLowerCase();
      if (ext === "mp4")  return "video/mp4";
      if (ext === "mov")  return "video/quicktime";
      if (ext === "webm") return "video/webm";
      if (ext === "m4v")  return "video/x-m4v";
      return "application/octet-stream";
    };

    const size    = head.size;
    const etag    = head.httpEtag || head.etag || "";
    const lastMod = head.uploaded?.toUTCString?.() || new Date().toUTCString();
    const ctype =
      head.httpMetadata?.contentType ||
      head.customMetadata?.contentType ||
      guessType(key);                 // <— this line is the important “step 3”

    // Handle Range (byte-range) requests for seeking
    const range = request.headers.get("Range");
    if (range) {
      // e.g. "bytes=12345-" or "bytes=0-999999"
      const m = /bytes=(\d+)-(\d+)?/.exec(range);
      const start = m ? parseInt(m[1], 10) : 0;
      const end   = m && m[2] ? Math.min(parseInt(m[2], 10), size - 1) : size - 1;
      if (start >= size || start > end) {
        return new Response(null, {
          status: 416,
          headers: { "Content-Range": `bytes */${size}` }
        });
      }
      const len = end - start + 1;
      const obj = await env.R2_BUCKET.get(key, { range: { offset: start, length: len } });
      if (!obj) return new Response("Not found", { status: 404 });

      return new Response(obj.body, {
        status: 206,
        headers: {
          "Content-Type": ctype,
          "Content-Length": String(len),
          "Accept-Ranges": "bytes",
          "Content-Range": `bytes ${start}-${end}/${size}`,
          ETag: etag,
          "Last-Modified": lastMod,
          "Cache-Control": "no-store"
        }
      });
    }

    // No Range: return full body
    const obj = await env.R2_BUCKET.get(key);
    if (!obj) return new Response("Not found", { status: 404 });

    return new Response(obj.body, {
      status: 200,
      headers: {
        "Content-Type": ctype,
        "Content-Length": String(size),
        "Accept-Ranges": "bytes",
        ETag: etag,
        "Last-Modified": lastMod,
        "Cache-Control": "no-store"
      }
    });
  } catch (err) {
    return new Response("Server error", { status: 500 });
  }
};

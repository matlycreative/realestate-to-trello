// /functions/api/sample.js
// Looks up pointers/<id>.json in R2 and returns a stream URL to the video.
// { streamUrl, company, link, foundKey?, error? }

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const id = url.searchParams.get("id");
    if (!id) return json({ streamUrl: null, company: null, link: null, error: "MISSING_ID" });

    const base = (env.PUBLIC_BASE && env.PUBLIC_BASE.trim()) || url.origin;
    const link = `${base.replace(/\/$/, "")}/p/?id=${encodeURIComponent(id)}`;

    // ---- Load pointer ----
    const pointerKey = `pointers/${id}.json`;
    const pointerObj = await env.R2_BUCKET.get(pointerKey);
    if (!pointerObj) return json({ streamUrl: null, company: null, link, error: "POINTER_NOT_FOUND" });

    const pointer = await pointerObj.json();
    let realKey = (pointer && pointer.key) || "";
    if (!realKey) return json({ streamUrl: null, company: pointer.company || null, link, error: "EMPTY_KEY" });

    // ---- Resolve the actual object path ----
    let head = await env.R2_BUCKET.head(realKey);

    // Try accidental nested path: "<key>/<filename>"
    if (!head) {
      const baseKey = realKey.replace(/\/+$/, "");
      const filename = baseKey.split("/").pop();
      const nestedKey = `${baseKey}/${filename}`;
      const headNested = await env.R2_BUCKET.head(nestedKey);
      if (headNested) { realKey = nestedKey; head = headNested; }
    }

    // Final fallback: list by prefix
    if (!head) {
      const list = await env.R2_BUCKET.list({ prefix: realKey.endsWith("/") ? realKey : realKey + "/", limit: 1 });
      if (list?.objects?.length) {
        realKey = list.objects[0].key;
      } else {
        return json({ streamUrl: null, company: pointer.company || null, link, error: "OBJECT_NOT_FOUND", wantedKey: realKey });
      }
    }

    // Same-origin streaming route (no presign needed)
    const streamUrl = `/api/stream?key=${encodeURIComponent(realKey)}`;
    return json({ streamUrl, company: pointer.company || null, link, foundKey: realKey });
  } catch (err) {
    return json({ streamUrl: null, company: null, link: null, error: "SERVER", message: String(err) });
  }
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
  });
}

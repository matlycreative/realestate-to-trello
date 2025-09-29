// /functions/api/sample.js
// Reads pointer JSON from R2 at `pointers/<id>.json` and returns:
// { signedUrl, company, link, foundKey }
// Works with both flat keys ("videos/a__b.mp4") and
// accidental nested keys ("videos/a__b.mp4/a__b.mp4").

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const id = url.searchParams.get("id"); // safe email: lowercased, @/. -> _
    if (!id) {
      return new Response("Missing id", { status: 400 });
    }

    // Build a public link (for debugging/comfort)
    const base = (env.PUBLIC_BASE && env.PUBLIC_BASE.trim()) || url.origin;
    const link = `${base.replace(/\/$/, "")}/p/?id=${encodeURIComponent(id)}`;

    // ---- Load pointer ----
    const pointerKey = `pointers/${id}.json`;
    const pointerObj = await env.R2_BUCKET.get(pointerKey);
    if (!pointerObj) {
      return json({ signedUrl: null, company: null, link, error: "POINTER_NOT_FOUND" }, 404);
    }

    const pointer = await pointerObj.json();
    let realKey = (pointer && pointer.key) || "";
    if (!realKey) {
      return json({ signedUrl: null, company: pointer.company || null, link, error: "EMPTY_KEY" }, 404);
    }

    // ---- Resolve the actual object path ----
    // 1) Try the flat key as-is
    let head = await env.R2_BUCKET.head(realKey);

    // 2) If not found, try the "nested" mistake: "<key>/<filename>"
    if (!head) {
      const baseKey = realKey.replace(/\/+$/, "");
      const filename = baseKey.split("/").pop();
      const nestedKey = `${baseKey}/${filename}`;
      const headNested = await env.R2_BUCKET.head(nestedKey);
      if (headNested) {
        realKey = nestedKey;
        head = headNested;
      }
    }

    // 3) Last resort: list the prefix
    if (!head) {
      const list = await env.R2_BUCKET.list({
        prefix: realKey.endsWith("/") ? realKey : realKey + "/",
        limit: 1
      });
      if (list && list.objects && list.objects.length) {
        realKey = list.objects[0].key;
      } else {
        return json({ signedUrl: null, company: pointer.company || null, link, error: "OBJECT_NOT_FOUND", wantedKey: realKey }, 404);
      }
    }

    // ---- Create a 24h signed URL ----
    const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, {
      method: "GET",
      expiry: 24 * 60 * 60
    });

    return json({ signedUrl, company: pointer.company || null, link, foundKey: realKey });
  } catch (err) {
    return new Response("Server error", { status: 500 });
  }
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
}

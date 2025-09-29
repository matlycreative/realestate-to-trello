// /functions/api/sample.js
// Reads pointer JSON from R2 at `pointers/<id>.json` and returns:
// { signedUrl, company, link, foundKey?, error? }

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const id = url.searchParams.get("id"); // safe email: lowercased, @/. -> _
    if (!id) {
      return json({ signedUrl: null, company: null, link: null, error: "MISSING_ID" });
    }

    const base = (env.PUBLIC_BASE && env.PUBLIC_BASE.trim()) || url.origin;
    const link = `${base.replace(/\/$/, "")}/p/?id=${encodeURIComponent(id)}`;

    // ---- Load pointer ----
    const pointerKey = `pointers/${id}.json`;
    const pointerObj = await env.R2_BUCKET.get(pointerKey);
    if (!pointerObj) {
      return json({ signedUrl: null, company: null, link, error: "POINTER_NOT_FOUND" });
    }

    const pointer = await pointerObj.json();
    let realKey = (pointer && pointer.key) || "";
    if (!realKey) {
      return json({ signedUrl: null, company: pointer.company || null, link, error: "EMPTY_KEY" });
    }

    // ---- Resolve the actual object path ----
    let head = await env.R2_BUCKET.head(realKey);

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

    if (!head) {
      const list = await env.R2_BUCKET.list({ prefix: realKey.endsWith("/") ? realKey : realKey + "/", limit: 1 });
      if (list && list.objects && list.objects.length) {
        realKey = list.objects[0].key;
      } else {
        return json({
          signedUrl: null,
          company: pointer.company || null,
          link,
          error: "OBJECT_NOT_FOUND",
          wantedKey: realKey
        });
      }
    }

    // ---- Create a 24h signed URL ----
    const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, {
      method: "GET",
      expiry: 24 * 60 * 60,
    });

    return json({ signedUrl, company: pointer.company || null, link, foundKey: realKey });
  } catch (err) {
    // Keep it 200 so the client can show a friendly message
    return json({ signedUrl: null, company: null, link: null, error: "SERVER", message: String(err) });
  }
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  });
}

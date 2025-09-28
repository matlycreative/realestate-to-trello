export const onRequestGet = async ({ request, env }) => {
  const url = new URL(request.url);
  const id = url.searchParams.get("id");               // was "slug"
  if (!id) return new Response("Bad", { status: 400 });

  const pointerKey = `pointers/${id}.json`;            // was pointers/<slug>.json
  const pointerObj = await env.R2_BUCKET.get(pointerKey);
  if (!pointerObj) {
    return new Response(JSON.stringify({ signedUrl: null, company: null }), {
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  const pointer = await pointerObj.json();
  const realKey = pointer.key;
  const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, { method: "GET", expiry: 24*3600 });

  return new Response(JSON.stringify({ signedUrl, company: pointer.company || null }), {
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
};

  const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, { method: "GET", expiry: 24*3600 });
  return new Response(JSON.stringify({ signedUrl, company: pointer.company || null }), {
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
};

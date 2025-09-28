export const onRequestGet = async ({ request, env }) => {
  const url = new URL(request.url);
  const slug = url.searchParams.get("slug");
  if (!slug) return new Response("Bad", { status: 400 });

  // pointer lives at samples/pointers/<slug>.json and contains {"key":"samples/videos/<filename>"}
  const pointerKey = `pointers/${slug}.json`;
  const pointerObj = await env.R2_BUCKET.get(pointerKey);
  if (!pointerObj) {
    return new Response(JSON.stringify({ signedUrl: null, company: null }), {
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  const pointer = await pointerObj.json();
  const realKey = pointer.key;
  if (!realKey) {
    return new Response(JSON.stringify({ signedUrl: null, company: null }), {
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, { method: "GET", expiry: 24*3600 });
  return new Response(JSON.stringify({ signedUrl, company: pointer.company || null }), {
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
};

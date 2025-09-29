// /functions/api/sample.js
// Reads pointer JSON from R2 at `pointers/<id>.json`, returns 24h signed URL + company + link
// Example page link: https://<your-project>.pages.dev/p/?id=jane_acme_com

export const onRequestGet = async ({ request, env }) => {
  try {
    const url = new URL(request.url);
    const id = url.searchParams.get("id"); // safe email: lowercased, @/. -> _
    if (!id) {
      return new Response("Missing id", { status: 400 });
    }

    // Build a public link for convenience (base from env, else current origin)
    const base = (env.PUBLIC_BASE && env.PUBLIC_BASE.trim()) || `${url.origin}`;
    const link = `${base.replace(/\/$/, "")}/p/?id=${encodeURIComponent(id)}`;

    // Pointer JSON created by your GitHub Action:
    // {
    //   "key": "videos/jane_acme_com__tour.mp4",
    //   "company": "Acme Homes"
    // }
    const pointerKey = `pointers/${id}.json`;
    const pointerObj = await env.R2_BUCKET.get(pointerKey);

    if (!pointerObj) {
      return new Response(
        JSON.stringify({ signedUrl: null, company: null, link }),
        { headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } }
      );
    }

    const pointer = await pointerObj.json();
    const realKey = pointer.key;
    if (!realKey) {
      return new Response(
        JSON.stringify({ signedUrl: null, company: pointer.company || null, link }),
        { headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } }
      );
    }

    // Create a 24-hour signed URL for the actual video object
    const signedUrl = await env.R2_BUCKET.createSignedUrl(realKey, {
      method: "GET",
      expiry: 24 * 60 * 60, // seconds
    });

    return new Response(
      JSON.stringify({ signedUrl, company: pointer.company || null, link }),
      { headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } }
    );
  } catch (err) {
    return new Response("Server error", { status: 500 });
  }
};

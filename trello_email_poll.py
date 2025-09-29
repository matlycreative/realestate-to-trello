def send_email(to_email: str, subject: str, body_text: str, *, link_url: str = "", link_text: str = "", link_color: str = ""):
    from email.message import EmailMessage
    import smtplib

    # Normalize link pieces
    if link_url and not re.match(r"^https?://", link_url, flags=re.I):
        link_url = "https://" + link_url
    label = (link_text or "My portfolio")

    # --- Plain-text body: show label instead of naked URL when disabled
    if link_url and not INCLUDE_PLAIN_URL:
        body_text_pt = body_text.replace(link_url, label)
    else:
        body_text_pt = body_text if (not link_url) else (
            body_text if link_url in body_text else (body_text.rstrip() + "\n\n" + link_url).strip()
        )

    # --- HTML body via marker trick: prevent the raw URL from ever appearing
    # 1) Replace the URL with a unique marker BEFORE escaping
    MARK = "__LINK_MARKER__"
    body_marked = body_text.replace(link_url, MARK) if link_url else body_text

    # 2) Convert to basic HTML
    html_core = text_to_html(body_marked)

    # 3) Auto-link any other URLs (not our marker)
    html_core = _autolink_html(html_core)

    # 4) Replace the escaped marker with a friendly anchor
    if link_url:
        esc_mark = html.escape(MARK)
        esc_href = html.escape(link_url, quote=True)
        style_attr = f' style="color:{html.escape(link_color)};text-decoration:underline;"' if link_color else ""
        anchor_html = f'<a{style_attr} href="{esc_href}">{html.escape(label)}</a>'
        html_core = html_core.replace(esc_mark, anchor_html)

    # Signature + message assembly
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_text_pt)                   # plain text (no naked URL if disabled)
    msg.add_alternative(html_full, subtype="html")  # HTML

    # Optional inline logo
    if SIGNATURE_INLINE and SIGNATURE_LOGO_URL:
        try:
            r = SESS.get(SIGNATURE_LOGO_URL, timeout=20)
            r.raise_for_status()
            data = r.content
            ctype = r.headers.get("Content-Type") or mimetypes.guess_type(SIGNATURE_LOGO_URL)[0] or "image/png"
            if not ctype.startswith("image/"):
                ctype = "image/png"
            maintype, subtype = ctype.split("/", 1)
            msg.get_payload()[-1].add_related(data, maintype=maintype, subtype=subtype, cid=logo_cid)
        except Exception as e:
            print(f"Inline logo fetch failed, sending without embed: {e}")

    # SMTP send with retry
    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USE_TLS:
                    s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
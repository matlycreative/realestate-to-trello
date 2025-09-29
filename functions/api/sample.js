- name: Upload videos in uploads/, write pointers, export email/link vars
  env:
    PUBLIC_BASE: ${{ secrets.PUBLIC_BASE }}
    R2_BUCKET:   ${{ secrets.R2_BUCKET }}
  run: |
    set -e
    shopt -s nullglob

    make_safe () { echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[@.]/_/g'; }

    files=(uploads/*)
    if [ ${#files[@]} -eq 0 ]; then
      echo "No files in uploads/ — nothing to do."
      exit 0
    fi

    for f in "${files[@]}"; do
      [ -f "$f" ] || continue
      base=$(basename "$f")                         # e.g. jane@acme.com__tour.mp4
      email="${base%%__*}"                          # e.g. jane@acme.com
      rest="${base#*__}"                            # e.g. tour.mp4
      safe=$(make_safe "$email")                    # e.g. jane_acme_com
      SAFE_UPPER=$(echo "$safe" | tr '[:lower:]' '[:upper:]')

      # Skip junk
      case "$base" in
        .DS_Store|*.tmp|*.part) echo "Skipping $base"; continue;;
      esac

      vid_key="videos/${safe}__${rest}"             # videos/jane_acme_com__tour.mp4
      echo "Uploading VIDEO -> r2:${R2_BUCKET}/${vid_key}"
      rclone copyto "$f" "r2:${R2_BUCKET}/${vid_key}" -vv

      # (optional) derive company …
      company=""
      if [ -n "${{ secrets.TRELLO_KEY }}" ] && [ -n "${{ secrets.TRELLO_TOKEN }}" ]; then
        if [ -n "${{ secrets.TRELLO_LIST_ID }}" ]; then
          curl -s "https://api.trello.com/1/lists/${{ secrets.TRELLO_LIST_ID }}/cards?fields=name,desc&key=${{ secrets.TRELLO_KEY }}&token=${{ secrets.TRELLO_TOKEN }}" > cards.json
        elif [ -n "${{ secrets.TRELLO_BOARD_ID }}" ]; then
          curl -s "https://api.trello.com/1/boards/${{ secrets.TRELLO_BOARD_ID }}/cards?fields=name,desc&key=${{ secrets.TRELLO_KEY }}&token=${{ secrets.TRELLO_TOKEN }}" > cards.json
        fi
        if [ -s cards.json ]; then
          card=$(jq -r --arg em "$email" '.[] | select(.desc|test($em;"i"))' cards.json | head -n 1)
          if [ -n "$card" ]; then
            desc=$(echo "$card" | jq -r '.desc')
            company=$(printf "%s\n" "$desc" | grep -i '^company:' | head -n 1 | sed 's/^company:[[:space:]]*//I')
          fi
        fi
      fi
      if [ -z "$company" ]; then
        domain=${email#*@}; company=$(echo "${domain%%.*}" | sed -E 's/[-_]/ /g; s/.*/\u&/')
      fi

      # ---- WRITE & UPLOAD POINTER JSON (this is the critical part) ----
      ptr_key="pointers/${safe}.json"
      printf '{"key":"%s","company":"%s"}\n' "$vid_key" "$company" > pointer.json
      echo "Uploading POINTER -> r2:${R2_BUCKET}/${ptr_key}"
      rclone copyto pointer.json "r2:${R2_BUCKET}/${ptr_key}" -vv

      # Export for reference (optional)
      echo "RECIPIENT_EMAIL_${SAFE_UPPER}=${email}" >> $GITHUB_ENV
      echo "COMPANY_${SAFE_UPPER}=${company}"       >> $GITHUB_ENV
      echo "LANDING_URL_${SAFE_UPPER}=${PUBLIC_BASE}/p/?id=${safe}" >> $GITHUB_ENV

      echo "::notice title=Prepared::Email to ${email} — link ${PUBLIC_BASE}/p/?id=${safe}"
    done

name: Trello Day0 Email (polling)

on:
  workflow_dispatch: {}
  schedule:
    - cron: "*/5 * * * *"  # every 5 minutes

permissions:
  contents: write

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    env:
      PYTHONUNBUFFERED: "1"

      # Trello
      TRELLO_KEY: ${{ secrets.TRELLO_KEY }}
      TRELLO_TOKEN: ${{ secrets.TRELLO_TOKEN }}
      TRELLO_LIST_ID_DAY0: ${{ secrets.TRELLO_LIST_ID_DAY0 }}

      # SMTP (Gmail example via app password)
      SMTP_HOST: ${{ secrets.SMTP_HOST }}          # e.g. smtp.gmail.com
      SMTP_PORT: ${{ secrets.SMTP_PORT }}          # e.g. 587
      SMTP_USERNAME: ${{ secrets.SMTP_USERNAME }}  # your gmail address
      SMTP_PASSWORD: ${{ secrets.SMTP_PASSWORD }}  # app password
      SMTP_USE_TLS: "1"

      FROM_NAME: ${{ secrets.FROM_NAME }}          # e.g. "Matthieu"
      FROM_EMAIL: ${{ secrets.FROM_EMAIL }}        # same as SMTP_USERNAME typically

      # Templates (optional — script has defaults if these are not set)
      SUBJECT_A: ${{ secrets.SUBJECT_A }}
      BODY_A: ${{ secrets.BODY_A }}
      SUBJECT_B: ${{ secrets.SUBJECT_B }}
      BODY_B: ${{ secrets.BODY_B }}

      # Internal cache path
      SENT_CACHE_FILE: ".data/sent_day0.jsonl"
      SENT_MARKER_TEXT: "day0 email sent"

    steps:
      - uses: actions/checkout@v4
        # keep default credentials so git-auto-commit can push
        with:
          fetch-depth: 0

      - name: Ensure data dir
        run: mkdir -p .data

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          python -m pip install requests

      - name: Run sender (poll)
        run: python trello_email_poll.py

      - name: Commit send cache
        if: ${{ always() }}
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "Update sent cache (day0) [skip ci]"
          file_pattern: .data/sent_day0.jsonl
          add_options: "-f"
          branch: ${{ github.ref_name }}
          create_branch: false

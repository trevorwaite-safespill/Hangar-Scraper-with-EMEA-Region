name: Safespill Weekly Hangar Report

on:
  # Every Monday at 8:00 AM PST = 16:00 UTC
  schedule:
    - cron: "0 16 * * 1"

  # Allow manual trigger from the GitHub Actions UI
  workflow_dispatch:

jobs:
  run-report:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run scraper and send report
        env:
          SERPAPI_KEY:       ${{ secrets.SERPAPI_KEY }}
          SAM_API_KEY:       ${{ secrets.SAM_API_KEY }}
          SMTP_USER:         ${{ secrets.SMTP_USER }}
          SMTP_PASSWORD:     ${{ secrets.SMTP_PASSWORD }}
          REPORT_RECIPIENT:  trevorw@safespill.com
        run: python scraper.py

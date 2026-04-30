@echo off
set DATABASE_URL=postgresql://postgres:ilgnvsEPQACcKJlnbzCqiZoeednFrXxo@switchyard.proxy.rlwy.net:13613/railway
cd /d C:\Users\T14s\ciudad3d-mcp
py -3.11 scraper_scheduler.py --max-pages 20

import asyncio, pandas as pd, json, os, argparse, gspread
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

class YCFounderScraper:
    def __init__(self, output_dir="d:/scrapping"):
        self.output_dir = output_dir
        self.base_url = "https://www.ycombinator.com"
        self.checkpoint_path = os.path.join(output_dir, "scraper_progress.json")
        self.json_path = os.path.join(output_dir, "yc_founders_data.json")
        self.excel_path = os.path.join(output_dir, "yc_live_sheet.xlsx")
        self.creds_path = os.path.join(output_dir, "service_account.json")
        self.sheet_id = "1p7u6nM7yMSmzscagEpxa2bE33jPyUoaXM4n7cHjsj18"
        self.records, self.processed = [], set()
        os.makedirs(output_dir, exist_ok=True)
        self._load_checkpoint()
        self.gs_client = self._init_gsheets()

    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, 'r', encoding='utf-8') as f:
                    cp = json.load(f)
                    self.records = cp.get("data", [])
                    self.processed = set(cp.get("processed_urls", []))
                print(f"Loaded: {len(self.records)} records.")
            except Exception as e: print(f"Checkpoint error: {e}")

    def _init_gsheets(self):
        if not os.path.exists(self.creds_path):
            print(f"Warning: {self.creds_path} not found. Skipping Google Sheets sync.")
            return None
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_path, scope)
            client = gspread.authorize(creds)
            return client.open_by_key(self.sheet_id).sheet1
        except Exception as e:
            print(f"GSheets Init Error: {e}")
            return None

    def save_live(self):
        """Saves JSON, Excel, and Google Sheets after every profile execution."""
        if not self.records: return
        try:
            # Prep data: Remove duplicates and handle nulls
            df = pd.DataFrame(self.records).fillna('')
            if 'linkedin' in df.columns: 
                df.drop_duplicates(subset=['linkedin'], keep='first', inplace=True)
            
            # Sync internal records to the deduplicated set
            cleaned_records = df.to_dict('records')

            # 1. Local JSON & Checkpoint
            with open(self.checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump({"data": cleaned_records, "processed_urls": list(self.processed)}, f, ensure_ascii=False, indent=2)
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_records, f, ensure_ascii=False, indent=2)

            # 2. Local Live Excel
            try:
                with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Founders')
                    ws = writer.sheets['Founders']
                    for i, col in enumerate(df.columns):
                        # Fix: Ensure all values are converted to string before measuring length
                        col_data = df[col].astype(str)
                        max_val_len = col_data.map(len).max() if not col_data.empty else 0
                        w = min(max(max_val_len, len(str(col))) + 2, 60)
                        ws.column_dimensions[chr(65 + i)].width = w
            except PermissionError: 
                print(f"Excel {self.excel_path} is open. Close it to sync.")

            # 3. Google Sheets (Real-time sync)
            if self.gs_client:
                # Sanitize for GSheets (convert all to strings to avoid "float" errors)
                header = df.columns.tolist()
                rows = df.values.tolist()
                # Ensure every cell is a string
                sanitized_rows = [[str(cell) for cell in row] for row in rows]
                self.gs_client.update('A1', [header] + sanitized_rows)
                
        except Exception as e: print(f"Save error: {e}")

    async def get_links(self, page, batch):
        print(f"Scanning batch: {batch}")
        try:
            await page.goto(f"{self.base_url}/founders?batches={batch}", wait_until="domcontentloaded", timeout=45000)
            # More natural scrolling
            for _ in range(12):
                await page.mouse.wheel(0, 1000)
                await asyncio.sleep(1.5)
            links = await page.evaluate("() => Array.from(document.querySelectorAll(\"a[href*='/founders/']\")).map(a => a.href).filter(h => !h.includes('?'))")
            return list(set(links))
        except Exception as e:
            print(f"Error getting links for {batch}: {e}")
            return []

    async def scrape_profile(self, page, url, retries=2):
        if url in self.processed: return
        
        for attempt in range(retries):
            try:
                # Use domcontentloaded for faster "live" feel, fall back to networkidle if needed
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait a tiny bit for heavy inertia data
                await asyncio.sleep(2)
                
                data = await page.evaluate('''() => {
                    const el = document.querySelector('[data-page]'), i = el ? JSON.parse(el.getAttribute('data-page')).props.company : {};
                    const links = Array.from(document.querySelectorAll('a'));
                    return {
                        name: i.founders?.[0]?.full_name || document.querySelector('h1')?.innerText.trim() || '',
                        linkedin: i.founders?.[0]?.linkedin_url || links.find(a => a.href.includes('linkedin.com/in/'))?.href || '',
                        companyName: i.name || '',
                        website: i.website || '',
                        batch: i.batch_name || '',
                        location: i.location || ''
                    };
                }''')
                
                if data['name'] or data['companyName']:
                    self.records.append(data)
                    self.processed.add(url)
                    print(f"   Scraped: {data['name']} @ {data['companyName']}")
                    self.save_live()
                    return # Success
                else:
                    raise ValueError("Empty data extracted")
                    
            except (TimeoutError, Exception) as e:
                if attempt < retries - 1:
                    print(f"   Retry {attempt+1} for {url}...")
                    await asyncio.sleep(3)
                else:
                    print(f"   Failed {url} after {retries} attempts: {e}")

    async def run(self, batches=None, headless=True):
        if not batches: batches = [f"W{y:02d}" for y in range(23, 26)] + [f"S{y:02d}" for y in range(23, 26)]
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            
            # Resource allocation optimization
            await page.route("**/*.{png,jpg,jpeg,svg,gif,woff,woff2,css}", lambda r: r.abort() if r.request.resource_type != "document" else r.continue_())
            
            all_links = []
            for b in batches: all_links.extend(await self.get_links(page, b))
            
            to_do = [l for l in list(set(all_links)) if l not in self.processed]
            print(f"Processing {len(to_do)} new profiles...")
            
            # Small batches to avoid memory swell
            for url in to_do: 
                await self.scrape_profile(page, url)
                await asyncio.sleep(0.5)
                
            await browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", nargs="+")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--no-headless", action="store_false", dest="headless")
    args = parser.parse_args()
    asyncio.run(YCFounderScraper().run(args.batches, args.headless))

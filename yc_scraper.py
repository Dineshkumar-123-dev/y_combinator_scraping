import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from O365 import Account, FileSystemTokenBackend

class YCFounderScraper:
    def __init__(self, output_dir="D:/webscrapping/y_combinator_scraping"):
        self.base_url = "https://www.ycombinator.com"
        self.output_dir = output_dir
        self.all_data = []
        self.processed_urls = set()
        self.checkpoint_file = os.path.join(output_dir, "scraper_progress.json")
        self.json_output = os.path.join(output_dir, "yc_founders_data.json")
        self.excel_output = os.path.join(output_dir, "yc_founders_data.xlsx")
        
        # GSheets Configuration
        self.creds_path = os.path.join(output_dir, "service_account.json")
        self.sheet_id = "1p7u6nM7yMSmzscagEpxa2bE33jPyUoaXM4n7cHjsj18"
        
        # SharePoint Configuration (Microsoft Graph)
        self.sharepoint_link = "https://zylen-my.sharepoint.com/:x:/g/personal/dineshkumar_k_zylensolutions_com/IQDDukcgsHKkRI3k_7_FhyMrAVU02dMPiZGxbIqJadR5vuo?e=IMs09n"
        self.client_id = "" # User needs to provide
        self.client_secret = "" # User needs to provide
        self.tenant_id = "" # User needs to provide
        
        os.makedirs(output_dir, exist_ok=True)
        self.load_checkpoint()
        self.gs_client = self._init_gsheets()
        self.sp_client = self._init_sharepoint()

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                    self.all_data = checkpoint.get("data", [])
                    # Ensure each entry in processed_urls is a string
                    self.processed_urls = set(str(url) for url in checkpoint.get("processed_urls", []))
                print(f"Loaded checkpoint: {len(self.all_data)} founders, {len(self.processed_urls)} URLs processed.")
            except Exception as e:
                print(f"Error loading checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "data": self.all_data,
                    "processed_urls": list(self.processed_urls)
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving checkpoint: {e}")

    def _init_gsheets(self):
        """Initializes the Google Sheets client."""
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

    def _init_sharepoint(self):
        """Initializes the SharePoint/O365 client."""
        if not self.client_id or not self.client_secret:
            return None
        try:
            credentials = (self.client_id, self.client_secret)
            token_path = os.path.join(self.output_dir, '.o365_token')
            account = Account(credentials, tenant_id=self.tenant_id, token_backend=FileSystemTokenBackend(token_path=self.output_dir, token_filename='.o365_token'))
            if not account.is_authenticated:
                print("SharePoint: App registered but not yet authenticated. Run manually to trigger OAuth flow.")
                return None
            return account
        except Exception as e:
            print(f"SharePoint Init Error: {e}")
            return None

    def save_live(self):
        """Consolidated real-time saving for JSON, Excel, and Google Sheets."""
        if not self.all_data:
            return
            
        try:
            # Prepare and clean data
            df = pd.DataFrame(self.all_data).fillna('')
            if 'name' in df.columns:
                df = df[df['name'] != '']
            if 'name' in df.columns and 'companyName' in df.columns:
                # Deduplicate based on name and company to keep co-founders
                # Also include linkedin if available for better accuracy
                subset = ['name', 'companyName']
                if 'linkedin' in df.columns:
                    mask_valid_linkedin = (df['linkedin'] != '') & (df['linkedin'].notna())
                
                df.drop_duplicates(subset=subset, keep='first', inplace=True)
            
            cleaned_records = df.to_dict('records')

            # 1. Update JSON and Checkpoint
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"data": cleaned_records, "processed_urls": list(self.processed_urls)}, f, ensure_ascii=False, indent=2)
            with open(self.json_output, 'w', encoding='utf-8') as f:
                json.dump(cleaned_records, f, ensure_ascii=False, indent=2)

            # 2. Update Excel
            try:
                with pd.ExcelWriter(self.excel_output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Founders')
                    ws = writer.sheets['Founders']
                    for i, col in enumerate(df.columns):
                        col_data = df[col].astype(str)
                        max_len = col_data.map(len).max() if not col_data.empty else 0
                        width = min(max(max_len, len(str(col))) + 2, 60)
                        ws.column_dimensions[chr(65 + i)].width = width
            except PermissionError:
                print("Excel file is open. Close it to allow syncing.")

            # 3. Update Google Sheets
            if self.gs_client:
                header = df.columns.tolist()
                rows = df.values.tolist()
                sanitized_rows = [[str(cell) for cell in row] for row in rows]
                self.gs_client.update('A1', [header] + sanitized_rows)

            # 4. Update SharePoint (if configured)
            if self.sp_client:
                try:
                    storage = self.sp_client.storage()
                    # Redeem the sharing link to get the target folder or file
                    shared_item = storage.get_shared_item(self.sharepoint_link)
                    
                    if shared_item:
                        # If it's a file, we can upload directly to its parent or replace it
                        # For simplicity, we'll try to update the specific file
                        shared_item.upload(self.excel_output)
                except Exception as e:
                    print(f"SharePoint update error: {e}")

        except Exception as e:
            print(f"Live save error: {e}")

    def save_to_json(self):
        if not self.all_data:
            return
        try:
            with open(self.json_output, 'w', encoding='utf-8') as f:
                json.dump(self.all_data, f, ensure_ascii=False, indent=2)
            # print(f"Data saved to JSON: {self.json_output}")
        except Exception as e:
            print(f"Error saving JSON: {e}")

    async def get_founder_links(self, page, batch):
        url = f"{self.base_url}/founders?batches={batch}"
        print(f"Discovering founders for batch: {batch}")
        try:
            # Use domcontentloaded + extra sleep for consistent rendering of dynamic content
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(5)
            
            # Natural scrolling to trigger lazy loading
            for _ in range(10):
                await page.mouse.wheel(0, 1200)
                await asyncio.sleep(1)

            links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(href => {
                        const isProfile = href.includes('/founders/');
                        const isNotAction = !href.includes('?batches=') && !href.includes('/verify') && !href.includes('/apply');
                        return isProfile && isNotAction;
                    });
            }''')
            unique_links = list(set(links))
            print(f"Found {len(unique_links)} founder links in batch {batch}.")
            return unique_links
        except Exception as e:
            print(f"Error getting links for batch {batch}: {e}")
            return []

    async def scrape_company_details(self, page, company_url):
        """Scrapes company name and website from the company page."""
        if not company_url:
            return None, None
            
        # Ensure we are going to the base company URL, not a subpage
        base_url = company_url.split('/jobs')[0].split('#')[0].split('?')[0]
            
        try:
            await page.goto(base_url, wait_until="networkidle", timeout=60000)
            
            company_data = await page.evaluate('''() => {
                // Try to get name from the specific 2xl font class shown in DoorDash page
                // We want the one that is NOT "Jobs at ..."
                const nameEls = Array.from(document.querySelectorAll('.text-2xl.font-medium, h1.text-2xl, h1'));
                const nameEl = nameEls.find(el => {
                    const text = el.innerText.trim();
                    return text && !text.toLowerCase().includes('jobs at') && !['jobs', 'news', 'company'].includes(text.toLowerCase());
                });
                
                const name = nameEl ? nameEl.innerText.trim() : '';
                
                // Try to get website from aria-label
                const websiteLink = document.querySelector('a[aria-label="Company website"]');
                const website = websiteLink ? websiteLink.href : '';
                
                return { name, website };
            }''')
            return company_data['name'], company_data['website']
        except Exception as e:
            print(f"Error scraping company details from {base_url}: {e}")
            return None, None

    async def scrape_profile(self, page, profile_url, retries=2):
        if profile_url in self.processed_urls:
            return
        
        for attempt in range(retries):
            try:
                # Use domcontentloaded for faster performance
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                data = await page.evaluate('''(url) => {
                    const el = document.querySelector('[data-page]');
                    if (!el) return null;
                    const inertia = JSON.parse(el.getAttribute('data-page')).props.company;
                    const allLinks = Array.from(document.querySelectorAll('a'));
                    
                    let name = '';
                    let linkedin = '';
                    let website = '';
                    let companyName = '';
                    let companyPage = '';
                    let batch = '';
                    let location = '';

                    if (inertia) {
                        companyName = inertia.name || '';
                        website = inertia.website || '';
                        companyPage = inertia.ycdc_company_url ? ('https://www.ycombinator.com' + inertia.ycdc_company_url) : '';
                        batch = inertia.batch_name || '';
                        location = inertia.location || '';
                        
                        // Use the passed-in profile_url instead of window.location.href
                        // to ensure we have the slug even after a redirect
                        if (url.includes('/founders/') && inertia.founders) {
                            const rawSlug = url.split('/founders/')[1].split(/[?#]/)[0].replace(/\/$/, '').toLowerCase();
                            const possibleSlugs = [rawSlug, rawSlug.replace(/[^a-z0-9]/g, ''), rawSlug.split('-').join('')];
                            
                            const founder = inertia.founders.find(f => {
                                const fName = f.full_name.toLowerCase();
                                const fNameClean = fName.replace(/[^a-z0-9]/g, '');
                                const fSlug = fName.replace(/\s+/g, '-');
                                const fLinked = (f.linkedin_url || '').toLowerCase();
                                return possibleSlugs.some(s => 
                                    fNameClean.includes(s) || s.includes(fNameClean) || fLinked.includes(s) || fSlug.includes(s)
                                );
                            });
                            
                            if (founder) {
                                name = founder.full_name || '';
                                linkedin = founder.linkedin_url || '';
                            } else {
                                name = rawSlug.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                            }
                        }
                        
                        // Fallback: If name still empty, grab first founder
                        if (!name && inertia.founders && inertia.founders.length > 0) {
                            name = inertia.founders[0].full_name || '';
                            linkedin = inertia.founders[0].linkedin_url || '';
                        }
                    }

                    if (!name) name = document.querySelector('h1')?.innerText.trim() || '';
                    if (!linkedin) linkedin = allLinks.find(a => a.href.includes('linkedin.com/in/'))?.href || '';
                    
                    return { name, linkedin, companyName, companyPage, website, batch, location };
                }''', profile_url)

                if not data:
                    raise ValueError("No Inertia data found")

                if data['companyPage'] and not data['website']:
                    c_name, c_website = await self.scrape_company_details(page, data['companyPage'])
                    if c_name: data['companyName'] = c_name
                    if c_website: data['website'] = c_website

                # Allow saving even if name is empty, as long as company exists
                if data['name'] or data['companyName']:
                    self.all_data.append(data)
                    self.processed_urls.add(profile_url)
                    print(f"   Scraped: {data['name'] or 'Unknown'} | {data['companyName']} | {data['website']}")
                    self.save_live()
                    return
                else:
                    raise ValueError("Insufficient data")

            except Exception as e:
                if attempt < retries - 1:
                    print(f"   Retry {attempt+1} for {profile_url}: {e}")
                    await asyncio.sleep(3)
                else:
                    print(f"   Failed {profile_url} after {retries} attempts: {e}")

    def save_to_excel(self, filename=None):
        if not self.all_data:
            print("No data to save.")
            return
        
        if filename is None:
            filename = self.excel_output
        
        df = pd.DataFrame(self.all_data)
        # Handle empty names or duplicates
        if 'name' in df.columns and 'companyName' in df.columns:
            df.drop_duplicates(subset=['name', 'companyName'], keep='first', inplace=True)
        if 'name' in df.columns:
            df = df[df['name'] != '']
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Founders')
                
                # Auto-fit columns
                worksheet = writer.sheets['Founders']
                for idx, col in enumerate(df.columns):
                    column_len = df[col].astype(str).str.len().max()
                    column_len = max(column_len, len(col)) + 2
                    # Limit width
                    column_len = min(column_len, 60)
                    column_letter = chr(65 + idx)
                    worksheet.column_dimensions[column_letter].width = column_len

            print(f"SUCCESS! Data saved to: \n   JSON: {self.json_output}\n   Excel: {filename}")
            print(f"Total Records: {len(df)}")
        except Exception as e:
            print(f"Error saving Excel: {e}")

    async def run(self, batches=None, headless=False):
        if batches is None:
            # Generate all batches from 2005 to 2025
            batches = []
            for year in range(5, 26):
                # Standard batches
                batches.append(f"W{year:02d}")
                batches.append(f"S{year:02d}")
                
                # New expansion batches starting late 2024
                if year == 24:
                    batches.append("F24")
                elif year >= 25:
                    batches.append(f"X{year:02d}") # Spring
                    batches.append(f"F{year:02d}") # Fall
        
        print("="*70)
        print("Y COMBINATOR FOUNDERS SCRAPER")
        print("="*70)
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target Batches ({len(batches)}): {', '.join(batches)}")
        print(f"Coverage: 2005 to 2025")
        print("="*70)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            # Disable images/fonts but keep CSS for better JS rendering compatibility
            await page.route("**/*.{png,jpg,jpeg,svg,gif,woff,woff2}", lambda route: route.abort())

            try:
                all_links = []
                for batch in batches:
                    links = await self.get_founder_links(page, batch)
                    all_links.extend(links)
                
                unique_links = list(set(all_links))
                # Only process links we haven't already processed
                links_to_process = [link for link in unique_links if link not in self.processed_urls]
                
                print(f"Found {len(unique_links)} total links. {len(links_to_process)} remaining to scrape.")

                for url in links_to_process:
                    await self.scrape_profile(page, url)
                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"Fatal error during crawl: {e}")
            finally:
                await browser.close()
                self.save_checkpoint()
                self.save_to_json()
                self.save_to_excel()

import argparse

async def main():
    parser = argparse.ArgumentParser(description="Y Combinator Founder Scraper")
    parser.add_argument("--batches", nargs="+", help="Specific batches to scrape (e.g., W24 S24)")
    parser.add_argument("--headless", action="store_true", default=True, help="Run browser in headless mode (default: True)")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run browser in windowed mode")
    
    args = parser.parse_args()
    
    scraper = YCFounderScraper()
    await scraper.run(batches=args.batches, headless=args.headless)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user. Progress saved.")
    except Exception as e:
        print(f"\nScript failed: {e}")

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
import json
import os

class YCFounderScraper:
    def __init__(self, output_dir="d:/scrapping"):
        self.base_url = "https://www.ycombinator.com"
        self.output_dir = output_dir
        self.all_data = []
        self.processed_urls = set()
        self.checkpoint_file = os.path.join(output_dir, "scraper_progress.json")
        self.json_output = os.path.join(output_dir, "yc_founders_data.json")
        self.excel_output = os.path.join(output_dir, "yc_founders_data.xlsx")
        self.load_checkpoint()

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
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # Scroll to load all cards
            last_height = await page.evaluate("document.body.scrollHeight")
            scroll_attempts = 0
            while scroll_attempts < 20: # Limit scrolls per batch
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                scroll_attempts += 1

            links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(href => href.includes('/founders/') && !href.includes('?batches=') && !href.includes('/verify'));
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

    async def scrape_profile(self, page, profile_url):
        if profile_url in self.processed_urls:
            return
        
        try:
            await page.goto(profile_url, wait_until="networkidle", timeout=60000)
            
            data = await page.evaluate('''() => {
                const getInertiaData = () => {
                    const el = document.querySelector('[data-page]');
                    if (!el) return null;
                    try {
                        return JSON.parse(el.getAttribute('data-page'));
                    } catch (e) {
                        return null;
                    }
                };

                const inertia = getInertiaData();
                const allLinks = Array.from(document.querySelectorAll('a'));
                
                // --- Initial values ---
                let name = '';
                let linkedin = '';
                let website = '';
                let companyName = '';
                let companyPage = '';
                let batch = '';
                let location = '';

                // --- Try Inertia Source First (More reliable) ---
                if (inertia && inertia.props) {
                    const company = inertia.props.company;
                    if (company) {
                        companyName = company.name || '';
                        website = company.website || '';
                        companyPage = company.ycdc_company_url ? ('https://www.ycombinator.com' + company.ycdc_company_url) : '';
                        batch = company.batch_name || '';
                        location = company.location || '';
                        
                        // Try to find the specific founder if this is a founder URL
                        const url = window.location.href;
                        if (url.includes('/founders/') && company.founders) {
                            const slug = url.split('/founders/')[1].split(/[?#]/)[0];
                            const founder = company.founders.find(f => 
                                f.full_name.toLowerCase().replace(/\\s+/g, '-').includes(slug.toLowerCase()) ||
                                (f.linkedin_url && f.linkedin_url.includes(slug))
                            );
                            if (founder) {
                                name = founder.full_name || '';
                                linkedin = founder.linkedin_url || '';
                            }
                        }
                        
                        // Fallback to first founder if still empty
                        if (!name && company.founders && company.founders.length > 0) {
                            name = company.founders[0].full_name || '';
                            linkedin = company.founders[0].linkedin_url || '';
                        }
                    }
                }

                // --- DOM Fallback/Augmentation ---
                if (!name) {
                    const h1 = document.querySelector('h1');
                    if (h1 && !['Active Founders', 'Founders', 'Co-Founders', 'Former Founders'].includes(h1.innerText.trim())) {
                        name = h1.innerText.trim();
                    }
                }

                if (!companyName) {
                    const companyLinks = allLinks.filter(a => 
                        a.href.includes('/companies/') && 
                        !a.href.includes('/industry/') && 
                        !a.href.includes('/location/') && 
                        !a.href.includes('/batch/') &&
                        !a.innerText.toLowerCase().includes('jobs') && // Exclude "Jobs" link
                        a.innerText.trim().length > 0
                    );
                    
                    if (companyLinks.length > 0) {
                        companyLinks.sort((a, b) => b.innerText.length - a.innerText.length);
                        companyPage = companyLinks[0].href;
                        companyName = companyLinks[0].innerText.trim();
                    }
                }

                if (!linkedin) {
                    linkedin = allLinks.find(a => a.href.includes('linkedin.com/in/'))?.href || '';
                }

                if (!website) {
                    const websiteLink = document.querySelector('a[aria-label="Company website"]');
                    if (websiteLink) website = websiteLink.href;
                    
                    if (!website) {
                        website = allLinks.find(a => 
                            a.href.startsWith('http') && 
                            !a.href.includes('ycombinator.com') && 
                            !a.href.includes('linkedin.com') &&
                            !a.href.includes('twitter.com') &&
                            !a.href.includes('facebook.com') &&
                            !a.href.includes('instagram.com') &&
                            !a.href.includes('github.com')
                        )?.href || '';
                    }
                }

                const bodyText = document.body.innerText;
                if (!batch) {
                    const batchMatch = bodyText.match(/(WINTER|SUMMER) 20\\d{2}/i) || bodyText.match(/[WS]\\d{2}/);
                    batch = batchMatch ? batchMatch[0] : '';
                }
                
                if (!location) {
                    const locationMatch = bodyText.match(/[A-Z][a-z]+, [A-Z]{2}, [A-Z]{3}/) || 
                                         bodyText.match(/[A-Z][a-z]+, [A-Z][a-z]+/) ||
                                         bodyText.match(/Based in ([^\\n.]+)/);
                    location = locationMatch ? (locationMatch[1] || locationMatch[0]) : '';
                    if (location.length > 50) location = location.substring(0, 50);
                }

                return {
                    name,
                    linkedin,
                    companyName,
                    companyPage,
                    website,
                    batch,
                    location
                };
            }''')


            # ENHANCEMENT: If companyPage is found, visit it to get the official Company Name and Website as per user's request
            if data['companyPage']:
                c_name, c_website = await self.scrape_company_details(page, data['companyPage'])
                if c_name:
                    data['companyName'] = c_name
                if c_website:
                    data['website'] = c_website

            self.all_data.append(data)
            self.processed_urls.add(profile_url)
            print(f"   Scraped: {data['name'] or 'Unknown'} | {data['companyName']} | {data['website']}")
            
            # Save periodic checkpoint every 10 profiles (both JSON and Excel as requested)
            if len(self.processed_urls) % 10 == 0:
                self.save_checkpoint()
                self.save_to_json()
                self.save_to_excel()

        except Exception as e:
            print(f"Error scraping profile {profile_url}: {e}")

    def save_to_excel(self, filename=None):
        if not self.all_data:
            print("No data to save.")
            return
        
        if filename is None:
            filename = self.excel_output
        
        df = pd.DataFrame(self.all_data)
        # Handle empty names or duplicates
        if 'name' in df.columns:
            df = df[df['name'] != '']
        if 'linkedin' in df.columns:
            df.drop_duplicates(subset=['linkedin'], inplace=True)
        
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
            
            # Disable non-essential resources
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

from ..ScraperInterface import ScraperInterface
from firecrawl import Firecrawl
from zenrows import ZenRowsClient
from bs4 import BeautifulSoup
import time
import requests
import pandas as pd
import json
import os

class FbrefScraper(ScraperInterface):
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.data = []

    def fetch_page_crawler(self, url):
        try:
            firecrawl = Firecrawl(api_key="fc-3b051fa7de1249b3a9e5c9a7bc65e568")

            # Scrape a website (streaming response)
            doc = firecrawl.scrape(url, formats=["html"])
            
            # Handle streaming response - iterate through chunks
            html_content = ""
            for chunk in doc:
                if isinstance(chunk, dict):
                    if 'html' in chunk:
                        html_content += chunk['html']
                    elif 'content' in chunk:
                        html_content += chunk['content']
                elif isinstance(chunk, str):
                    html_content += chunk
            
            if html_content:
                print(f"✓ Successfully extracted HTML from {url}")
                return html_content
            else:
                raise ValueError("No HTML content extracted from streaming response")
        except Exception as e:
            print(f"Error fetching {url} with Firecrawl: {e}")
            return None

    def fetch_page(self, url):
        try:

            # client = ZenRowsClient("5939ba1ec76be7386aac4c3fd29e3940e567ff24")
            # params = {"js_render":"true","premium_proxy":"true"}

            # response = client.get(url, params=params)

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"

            }

            response = requests.get(url,headers=headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def parse_page(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')

        ## Getting the first stats table on the page
        table = soup.find_all('table', class_='stats_table')[0]

        ## Extracting all links references in the table 
        links = table.find_all('a')
        links = [l.get('href') for l in links]
        
        ## Filtering only squad links
        links = [l for l in links if '/squads/' in l]

        ## Forming complete URLs
        team_urls = [f"https://fbref.com{link}" for link in links ]

        for team in team_urls:
            ## Extracting the teams data
            team_name = team.split('/')[-1].replace('-Stats','')

            url_data = self.fetch_page(team)

            soup = BeautifulSoup(url_data, 'lxml')
            team_stats = soup.find_all('table', class_='stats_table')[0]

            if team_stats and team_stats.columns:
                team_stats.columns = team_stats.columns.droplevel()

            
            team_data = pd.read_html(str(team_stats))[0]
            team_data['Team'] = team_name
            self.data.append(team_data)
            time.sleep(5)  # Be polite and avoid overwhelming the server
        
    def save_to_json_realtime(self, team_data, team_name, filename='premier_league_data.json'):
        """Save team data to JSON file in real-time (appends each team)"""
        try:
            # Create output folder if it doesn't exist
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            # Convert dataframe to dictionary
            team_dict = team_data.to_dict(orient='records')
            
            # Check if file exists and has data
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            else:
                existing_data = []
            
            # Append new team data
            existing_data.extend(team_dict)
            
            # Write back to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
            print(f"✓ {team_name} saved to {filepath}")
        except Exception as e:
            print(f"Error saving {team_name} to JSON: {e}")
        
    def save_to_csv(self, filename):
        if self.data:
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, filename)
            stats_df = pd.concat(self.data)
            # Save with headers explicitly set to True
            stats_df.to_csv(filepath, index=False, header=True)
            print(f"Data saved to {filepath}")
        else:
            print("No data to save.")
    
    def json_to_csv(self, file_path):
        """Convert JSON file to CSV file"""
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                print(f"✗ Error: File not found at {file_path}")
                return None
            
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Generate CSV filename from JSON filename
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_dir = os.path.dirname(file_path) or 'output'
            csv_filename = f"{base_name}.csv"
            csv_filepath = os.path.join(output_dir, csv_filename)
            
            # Save to CSV with headers explicitly set to True
            df.to_csv(csv_filepath, index=False, header=True)
            print(f"✓ Successfully converted JSON to CSV")
            print(f"✓ Saved to {csv_filepath}")
            return csv_filepath
        except json.JSONDecodeError as e:
            print(f"✗ Error: Invalid JSON format - {e}")
            return None
        except Exception as e:
            print(f"✗ Error converting JSON to CSV: {e}")
            return None


    def scrape(self, pages=1, delay=2):
            print(f"Scraping {self.base_url}")
            html_content = self.fetch_page(self.base_url)
            if html_content:
                self.parse_page(html_content)
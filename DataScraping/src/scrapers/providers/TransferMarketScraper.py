from ..ScraperInterface import ScraperInterface
from firecrawl import Firecrawl
from zenrows import ZenRowsClient
from bs4 import BeautifulSoup
import time
import requests
import pandas as pd
import os

class TransferMarketScraper(ScraperInterface):
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.data = []

    def fetch_page(self, url):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
            }
        
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def parse_page(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup

    def extract_player_data(self, players_table, club_name):
        """Extract clean player data from the table"""

        if players_table and players_table.columns:
                players_table.columns = players_table.columns.droplevel()
                
        players_list = []
        rows = players_table.find_all('tbody')[0].find_all('tr', recursive=False)
 
        
        for row in rows:
            try:
                cells = row.find_all('td', recursive=False)

                # Extract jersey number
                jersey = cells[0].find('div', class_='rn_nummer')
                jersey_num = jersey.text.strip() if jersey else ''
                
                # Extract player name and position from the complex nested structure
                player_cell = cells[1]
                player_link = player_cell.find('td', {'class': 'hauptlink'}).find('a')
                player_name = player_link.text.strip() if player_link else ''
                
                # Extract position
                position_elem = player_cell.find_all('td')[-1] if player_cell.find_all('td') else None
                position = position_elem.text.strip() if position_elem else ''
                
                # Extract birth date/age
                birth_date = cells[2].text.strip() if len(cells) > 2 else ''
                
                # Extract nationality
                nat_cell = cells[3]
                nat_flags = nat_cell.find_all('img', {'class': 'flaggenrahmen'})
                nationality = ', '.join([img.get('title', '') for img in nat_flags]) if nat_flags else ''
                
                # Extract market value
                market_value = cells[4].find('a')
                market_value_text = market_value.text.strip() if market_value else ''
                

                players_list.append({
                        'No.': jersey_num,
                        'Player': player_name,
                        'Position': position,
                        'Date of birth/Age': birth_date,
                        'Nationality': nationality,
                        'Market value': market_value_text,
                        'Team': club_name
                })


            except Exception as e:
                print(f"Error parsing row: {e}")
                continue
        return pd.DataFrame(players_list)


    def scrape(self):
        page_content = self.fetch_page(self.base_url)
        if page_content:
            soup = self.parse_page(page_content)

            table = soup.find_all('table', {'class': 'items'})[0]

            tds_with_links = table.find_all('td', {'class': 'hauptlink no-border-links'})

            links = [
                (td.find('a').text.strip(),td.find('a').get('href')) for td in tds_with_links 
            ]
            print(links)
            for club_name,link in links:
                full_url = f"https://www.transfermarkt.com{link}"
                print(f"Scraping: {full_url}")

                club_page = self.fetch_page(full_url)

                html_soup = self.parse_page(club_page)

                players_table = html_soup.find_all('table', {'class': 'items'})[0]

                # Use the new manual parser instead of pd.read_html()
                players_data = self.extract_player_data(players_table, club_name)
                print(players_data)

                if not players_data.empty:
                    self.data.append(players_data)
                    print(f"✓ Scraped {len(players_data)} players from {club_name}")
                else:
                    print(f"⚠ No players found for {club_name}")
                
                time.sleep(5)
        else:
            print("Failed to retrieve page content.")
    
    def save_to_csv(self, filename):
        if self.data:
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, filename)
            
            full_data = pd.concat(self.data, ignore_index=True)
            
            # Sort by team and player name for better organization
            if 'Team' in full_data.columns and 'Player' in full_data.columns:
                full_data = full_data.sort_values(['Team', 'Player']).reset_index(drop=True)
            
            # Save with headers explicitly set to True
            full_data.to_csv(filepath, index=False, header=True)
            print(f"✓ Data saved to {filepath}")
            print(f"✓ Total records: {len(full_data)}")
        else:
            print("No data to save.")
    
    
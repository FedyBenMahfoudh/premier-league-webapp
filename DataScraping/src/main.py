from scrapers.providers.FbrefScraper import FbrefScraper
from scrapers.providers.TransferMarketScraper import TransferMarketScraper
from DataScraping.src.utils.clean import clean_dataframe
if __name__ == '__main__':

    # base_url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    # scraper = FbrefScraper(base_url=base_url)

    # page_content = scraper.fetch_page(base_url)
    # if page_content:
    #     scraper.parse_page(page_content)
    #     scraper.save_to_csv("premier_league_stats.csv")   

    base_url = "https://www.transfermarkt.com/premier-league/daten/wettbewerb/GB1"

    scraper = TransferMarketScraper(base_url=base_url)
    scraper.scrape()
    scraper.save_to_csv("transfermarket_premier_league_stats.csv")
    cl
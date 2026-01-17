import pandas as pd
import requests
import time 
from abc import ABC, abstractmethod


class ScraperInterface(ABC):
    @abstractmethod
    def fetch_page(self, url):
        pass

    @abstractmethod
    def parse_page(self, html_content):
        pass

    @abstractmethod
    def scrape(self, pages=1, delay=2):
        pass

    @abstractmethod
    def save_to_csv(self, filename):
        pass
#!/usr/bin/env python
# coding: utf-8

import requests
import time
import csv
import re
from bs4 import BeautifulSoup
import random
import hashlib


def write_to_csv(list_input): # writes book line to .csv
    
    list_input.append(hashlib.md5(bytes(list_input[0], 'utf8')).hexdigest()) # book_id

    # publisher_id
    try:
        with open("data/publishers.csv") as f:
            lines = f.readlines()
    except:
        return False

    random_line = random.choice(lines).strip()
    list_input.append(random_line.split(';')[0])

    list_input.append(random.randint(2000, 2020)) #publication year
    list_input.append(random.randint(100, 500)) # number of pages
    list_input.append(random.randint(0, 10)) # amount

    #author_id
    try:
        with open("data/authors.csv") as f:
            lines = f.readlines()
    except:
        return False

    random_line = random.choice(lines).strip()
    list_input.append(random_line.split(';')[0])


    try:
        with open("data/books.csv", "a") as fopen:
            csv_writer = csv.writer(fopen, delimiter=";")
            csv_writer.writerow(list_input)
    except:
        return False


def scrape(source_url, soup):  # scrapes URLs
    
    books = soup.find_all("article", class_="product_pod")

    # Iterate over each book article tag
    for each_book in books:
        info_url = source_url+"/"+each_book.h3.find("a")["href"] # info URL

        title = each_book.h3.find("a")["title"] # book title

        # book price
        price = each_book.find("p", class_="price_color").text.strip().encode("ascii", "ignore").decode("ascii")
        
        write_to_csv([info_url, title,  price]) #invoke writing to csv


def browse_and_scrape(seed_url, page_number=1):
    
    url_pat = re.compile(r"(http://.*\.com)")
    source_url = url_pat.search(seed_url).group(0) # Fetch the URL - appending info pages

    formatted_url = seed_url.format(str(page_number)) # Page_number inserting to URL from __main__ function

    try:
        html_text = requests.get(formatted_url).text
        
        soup = BeautifulSoup(html_text, "html.parser") # Prepare the soup
        print(f"Now Scraping - {formatted_url}")

       
        if soup.find("li", class_="next") != None:  # stops the script when it founds an empty page
            scrape(source_url, soup)
           
            time.sleep(3)
            page_number += 1
            
            browse_and_scrape(seed_url, page_number) # Recursively invoke 'browse_and_scrape' function
        else:
            scrape(source_url, soup)     # The script exits here
            return True
        return True
    except Exception as e:
        return e


if __name__ == "__main__":
    seed_url = "http://books.toscrape.com/catalogue/page-{}.html"
    print("Web scraping has begun")
    result = browse_and_scrape(seed_url)
    if result == True:
        print("Web scraping is complete!")
    else:
        print(f"Web scraping has fallen! - {result}")
 

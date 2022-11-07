from common.config import Config
from common.template import SqlTemplate
import asyncio
from asyncio.log import logger
import sqlite3
import logging
import aiohttp
from requests import get
import json
import hashlib
from collections import defaultdict, Counter
from jinja2 import Template
import re
import csv
import os

LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)
current_path = os.path.dirname(os.path.realpath(__file__))


def init_db():
    # Inititalize the database, create cat facts,word count unicode table
    # Create country table and load data
    try:
        os.remove(current_path+"/"+"dbs/interview.db")
    except OSError:
        pass

    log.info(f"remove database file")

    country_data = []

    # Read country data from csv file
    with open(current_path+"/"+"/country.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_data.append((row["Name"], row["Nationality"]))

    ddls = [Config.COUNTRY_DDL, Config.CAT_FACT_DDL,
            Config.CAT_FACT_UNICODE_DDL, Config.CAT_FACT_WORDS_DDL]
    # Create database and tables, load country data
    with sqlite3.connect(current_path+"/"+'dbs/interview.db') as conn:
        cursor = conn.cursor()
        for ddl in ddls:
            cursor.execute(ddl)
        cursor.executemany("INSERT INTO COUNTRIES VALUES(?, ?)", country_data)

    log.info("table created and country data loaded")


async def get_fact(url: str, rate, session) -> str:
    # Get fact data from API asynchronously
    try:
        async with rate:
            async with session.request(method="GET", url=url) as response:
                data = await response.text()
                status = response.status
                if status != 200:
                    log.warning(
                        f"Request failed for {url} with status code {status}")
                await asyncio.sleep(Config.API_REQUEST_INTERVAL)
                return data
    except Exception as e:
        log.warning(e)


async def load_fact_data_to_dict():

    init_response = get("https://catfact.ninja/facts")
    total_page = json.loads(init_response.text)["last_page"]

    base_url = "https://catfact.ninja/facts?page={{page_number}}"
    urls = [Template(base_url).render(page_number=i+1)
            for i in range(total_page)]
    # To store raw response fact data
    facts_list = []
    # Store the fact data with Haskey to remove duplication
    facts_dict = defaultdict(lambda: "fact's hashkey not found")

    # Create a semaphore to limit the number of concurrent requests, by default 10
    rate = asyncio.Semaphore(Config.API_RATE_LIMIT)
    async with aiohttp.ClientSession() as session:
        requests = [get_fact(url=url, rate=rate, session=session)
                    for url in urls]
        response_results = await asyncio.gather(*requests)

    # Store the raw response data if result not None
    for result in response_results:
        if result:
            raw_data = json.loads(result)
            if "data" in raw_data.keys():
                facts_list += raw_data["data"]

    # Remove duplication by using fact's hashkey
    fact_count = 0
    for fact in facts_list:
        facts_dict[hashlib.sha1(fact["fact"].encode(
            "utf-8")).hexdigest()] = fact["fact"]
        fact_count += 1
    log.info(
        f"Total fact count read from `https://catfact.ninja/facts`: {fact_count}")

    return facts_dict


def main():
    init_db()

    facts_data = asyncio.run(load_fact_data_to_dict())
    facts_words_data_sql = []
    facts_unicode_data_sql = []

    for hashkey, fact in facts_data.items():
        # Save the words after removing special characters
        words_remove_special_chars = re.sub(
            r"[^a-zA-Z- -'-‘-’]", "", fact).split(" ")

        # Remove empty string from split result
        word_counts = Counter(
            [word for word in words_remove_special_chars if word])
        char_counts = Counter(fact)

        facts_words_data_sql += [(hashkey, word, count)
                                 for word, count in word_counts.items()]

        # Store Unicode
        facts_unicode_data_sql += [(hashkey, char, count)
                                   for char, count in char_counts.items()]

    # convert facts data format to be inserted into sql
    facts_data_sql = [(hashkey, fact)
                      for hashkey, fact in facts_data.items()]

    with sqlite3.connect(current_path+"/"+"dbs/interview.db") as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO CAT_FACTS VALUES(?, ?)", facts_data_sql)
        log.info("cat facts data loaded")
        cursor.executemany(
            "INSERT INTO CAT_FACT_WORD_COUNT VALUES(?, ?, ?)", facts_words_data_sql)
        log.info("cat facts word count data loaded")
        cursor.executemany(
            "INSERT INTO CAT_FACT_UNICODE_COUNT VALUES(?, ?, ?)", facts_unicode_data_sql)
        log.info("cat facts unicode count data loaded")

    with sqlite3.connect(current_path+"/"+"dbs/interview.db") as conn:

        try:
            cursor = conn.cursor()
            cursor.execute(SqlTemplate.get_number_of_words_sql())
            no_of_words = cursor.fetchall()[0][0]
            log.info(f"Total number of words: {no_of_words}")
        except Exception as e:
            log.warning(e)

        try:
            cursor.execute(
                SqlTemplate.get_nth_common_or_least_unicode())
            temp = cursor.fetchall()
            most_common_unicode = temp[0][0]
            most_common_unicode_count = temp[0][1]
            log.info(
                f"Most common unicode is `{most_common_unicode}` and count is {most_common_unicode_count}")
        except Exception as e:
            log.warning(e)

        try:
            cursor.execute(
                SqlTemplate.get_top_bottom_n_words(top_nth_word=20, most_common=True))
            top_20_words = cursor.fetchall()
            log.info(f"Top 20 words and count: {top_20_words}")
        except Exception as e:
            log.warning(e)

        try:
            cursor.execute(
                SqlTemplate.get_nth_common_or_least_country())
            temp = cursor.fetchall()
            most_common_country = temp[0][0]
            most_common_country_count = temp[0][1]
            log.info(
                f"Most common country is {most_common_country} and count is {most_common_country_count}")

        except Exception as e:
            log.warning(e)


if __name__ == "__main__":
    main()

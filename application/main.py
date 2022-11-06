from common.config import Config
import asyncio
from asyncio.log import logger
import sqlite3
import logging
import aiohttp
from async_timeout import timeout
from requests import request, session, get
import json
import hashlib
from collections import defaultdict, Counter
from jinja2 import Template
import logging
import re
import csv
import os

LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)


def init_db():

    try:
        os.remove("dbs/interview.db")
    except OSError:
        pass

    country_data = []
    with open("country.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_data.append((row["Name"], row["Nationality"]))

    ddls = [Config.COUNTRY_DDL, Config.CAT_FACT_DDL,
            Config.CAT_FACT_UNICODE_DDL, Config.CAT_FACT_WORDS_DDL]
    with sqlite3.connect('dbs/interview.db') as conn:
        cursor = conn.cursor()
        for ddl in ddls:
            cursor.execute(ddl)
        cursor.executemany("INSERT INTO COUNTRYS VALUES(?, ?)", country_data)


async def get_fact(url: str, rate, session) -> str:
    try:
        async with rate:
            async with session.request(method="GET", url=url) as response:
                data = await response.text()
                status = response.status
                log.info(f"Make request: {url}. Status; {status}")
                # Only making API call in every 1/10 secs
                await asyncio.sleep(Config.API_REQUEST_INTERVAL)
                return data
    except Exception as e:
        print(e)


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

    # Limit 10 requests at same time
    rate = asyncio.Semaphore(Config.API_RATE_LIMIT)
    async with aiohttp.ClientSession() as session:
        requests = [get_fact(url=url, rate=rate, session=session)
                    for url in urls]
        response_results = await asyncio.gather(*requests)

    for result in response_results:
        if result:
            raw_data = json.loads(result)
            if "data" in raw_data.keys():
                facts_list += raw_data["data"]

    facts_with_key = []
    for fact in facts_list:
        facts_dict[hashlib.sha1(fact["fact"].encode(
            "utf-8")).hexdigest()] = fact["fact"]

    return facts_dict


def main():
    init_db()

    facts_data = asyncio.run(load_fact_data_to_dict())
    facts_words_data_sql = []
    facts_unicode_data_sql = []

    for hashkey, fact in facts_data.items():
        # Save the words after removing special characters
        words_remove_special_chars = re.sub(
            r"[^a-zA-Z- -'-‘-’]", "", fact)
        word_counts = Counter(
            words_remove_special_chars.split(" "))
        char_counts = Counter(fact)

        facts_words_data_sql += [(hashkey, word, count)
                                 for word, count in word_counts.items()]

        # Store Unicode
        facts_unicode_data_sql += [(hashkey, char, count)
                                   for char, count in char_counts.items()]

    facts_data_sql = [(hashkey, fact)
                      for hashkey, fact in facts_data.items()]

    with sqlite3.connect('dbs/interview.db') as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO CAT_FACTS VALUES(?, ?)", facts_data_sql)
        cursor.executemany(
            "INSERT INTO CAT_FACT_WORD_COUNT VALUES(?, ?, ?)", facts_words_data_sql)
        cursor.executemany(
            "INSERT INTO CAT_FACT_UNICODE_COUNT VALUES(?, ?, ?)", facts_unicode_data_sql)
        cursor.execute("select * from CAT_FACT_UNICODE_COUNT")
        print(cursor.fetchall())


if __name__ == "__main__":
    main()

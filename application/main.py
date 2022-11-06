import asyncio
from asyncio.log import logger
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

LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)


async def get_fact(url: str, rate, session) -> str:
    try:
        async with rate:
            async with session.request(method="GET", url=url) as response:
                data = await response.text()
                status = response.status
                log.info(f"Make request: {url}. Status; {status}")
                # Sleep for 0.5 after each batch of request
                await asyncio.sleep(1/2)
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

    rate = asyncio.Semaphore(10)  # Limit 2 requests at same time
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
    facts_data = asyncio.run(load_fact_data_to_dict())
    facts_words_dict = defaultdict(lambda: "fact's hashkey not found")
    facts_unicode_dict = defaultdict(lambda: "fact's hashkey not found")

    for hashkey, fact in facts_data.items():
        # Save the words after removing special characters
        words_remove_special_chars = re.sub(
            r"[^a-zA-Z- -'-‘-’]", "", fact)
        facts_words_dict[hashkey] = Counter(
            words_remove_special_chars.split(" "))

        # Store Unicode
        facts_unicode_dict[hashkey] = dict(Counter(fact))

    facts_data_to_be_insert = [(hashkey, fact)
                               for hashkey, fact in facts_data.items()]
    command = """
            INSERT INTO "INTERVIEW"."CAT_FACT_WORD_COUNT" ("FACT_FK_KEY", "{{entity_name}}", "{{entity_count}}")
            VALUES 
            {% for hashkey, value in data.items() -%}
                {% for word, count in value.items() -%} 
	    			("{{hashkey}}","{{word}}",{{count}}){{",
                    " if not loop.last else "" }}
				{%- endfor %}{{",
                " if not loop.last else "" }}
             {%- endfor %}
    """
    CAT_FACT_WORD_META = {"table_name": "CAT_FACT_WORD_COUNT",
                          "entity_name": "WORD", "entity_count": "WORD_COUNT"}

    print(Template(command).render(data=facts_words_dict,
                                   **CAT_FACT_WORD_META)[:100])

    # for hashkey, words_and_count in facts_words_dict.items():
    #     for word, count in words_and_count.items():
    #         query = Template(command).render(
    #             hashkey=hashkey, word=word, word_count=count)


if __name__ == "__main__":
    main()

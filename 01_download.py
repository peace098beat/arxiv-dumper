from pathlib import Path

from urllib.parse import urlencode
from urllib.request import urlretrieve

import feedparser
import pandas as pd

import time


class SortCriterion():
    Relevance = "relevance"
    LastUpdatedDate = "lastUpdatedDate"
    SubmittedDate = "submittedDate"


class SortOrder():
    Ascending = "ascending"
    Descending = "descending"



class ArxivParser:
    base_url = 'http://export.arxiv.org/api/query?{}'

    def __init__(self, search_query='"time series"', sortBy="submittedDate", sortOrder="ascending"):
        self.url_args = {
            "search_query": search_query,
            "start": None,
            "max_results": None,
            "sortBy": sortBy,
            "sortOrder":  sortOrder
        }

    @classmethod
    def build_url(cls, url_args, start, max_results):
        url_args.update({
            "start":start,
            "max_results": max_results
        })
        url = cls.base_url.format(urlencode(url_args))
        return url

    def get(self, fetch_start, fetch_count):
        url = ArxivParser.build_url(self.url_args, fetch_start, fetch_count)

        feed = feedparser.parse(url)

        return feed.status, feed["entries"]



from pathlib import Path
import os


# class Db:
#     def __init__(self, csv_file):
#         self.csv_file = csv_file
#         Path(csv_file).parent.mkdir(exists=True, parents=True)
#     def clear_db(self):
#       if Path(csv_file).exists():
#         os.remove(csv_file)
#     def append_to_db(self, df):
#       if Path(csv_file).exists():
#         df.to_csv(csv_file, mode = 'a', header = False, index = False, compression='gzip')
#       else:
#         df.to_csv(csv_file, mode = 'w', header = True, index = False, compression='gzip')
#     def read_db(self):
#       df_read = pd.read_csv(csv_file, compression='gzip')
#       return df_read


def main():
    search_query='"time series"'

    arxiv_parser = ArxivParser(search_query=search_query)

    fetch_start = 0
    max_count = 1000

    fetch_count = 1000
    MAX_RETRY = 5
    DELAY = 5

    OVER_WRITE = False

    db_dir = "data"
    Path(db_dir).mkdir(exist_ok=True, parents=True)

    fetched_cnt = 0
    for fetch_start in range(fetch_start, max_count, fetch_count):

        # get 
        for retry in range(MAX_RETRY):
          
            status, entries = arxiv_parser.get(fetch_start=fetch_start, fetch_count=fetch_count)

            if len(entries) > 0:
                df_ent = pd.DataFrame(entries)

                # csv_file = f"{db_dir}/{fetch_start:06d}.csv.gzip"
                # df_ent.to_csv(csv_file, mode = 'w', header = True, index = False, compression='gzip')

                csv_file = f"{db_dir}/{fetch_start:06d}.csv"
                
                if Path(csv_file).exists():
                    if OVER_WRITE: 
                        os.remove(csv_file)
                    else:
                        print("skip")
                        break

                df_ent.to_csv(csv_file, mode = 'w', header = True, index = False)


                break
            else:
                # retry
                time.sleep(DELAY)
                continue

        # result of query
        res = {
            "search_query": search_query,
            "fetch_start": fetch_start,
            "fetch_count": fetch_count,
            "max_count": max_count,
            "status": status,
            "retry": retry,
            "entries": len(entries),
        }
        df_res = pd.DataFrame({0: res}).T

        if Path("result.csv").exists():
            df_res.to_csv("result.csv", mode="a", header=False, index=False)
        else:
            df_res.to_csv("result.csv", mode="w", header=True, index=False)
        print(res)


        fetched_cnt += fetch_count

        time.sleep(DELAY)


if __name__ == '__main__':
    main()
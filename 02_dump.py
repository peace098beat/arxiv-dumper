import pandas as pd
from pathlib import Path


csv_paths = Path("data").glob("*.csv")

buf = []
for csv_path in csv_paths:
    df_csv = pd.read_csv(csv_path)
    print(csv_path, len(df_csv))
    buf.append(df_csv)


df = pd.concat(buf).reset_index(drop=True)
assert df.duplicated(subset="id").sum() == 0,  df.duplicated(subset="id").sum() # check duplicate row

usecols = [
    'id',
    # 'guidislink',
    # 'link',
    # 'updated',
    # 'updated_parsed',
    'published',
    # 'published_parsed',
    'title',
    # 'title_detail',
    'summary',
    # 'summary_detail',
    'authors',
    # 'author_detail',
    'author',
    # 'links',
    'arxiv_primary_category',
    'tags',
    # 'arxiv_comment',
    # 'arxiv_doi',
    # 'arxiv_affiliation',
    # 'arxiv_journal_ref'
]


df = df[usecols]

df["published"] = pd.to_datetime(df["published"])
df = df.sort_values("published")

df["primary_category"] = df["arxiv_primary_category"].apply(lambda x: eval(x)["term"])

df["primary_category_head"] = df["primary_category"].apply(lambda x: x.split(".")[0])

df = df.set_index("published")

df = df["2015":"2020"]

target_category = ['cs', 'stat','eess','q-fin','econ']
m = df["primary_category_head"].isin(target_category)
df = df[m]

df["title"] = df["title"].replace("\n|\r", "", regex=True)
df["summary"] = df["summary"].replace("\n|\r", "", regex=True)


df.to_csv("agg_timeseries.csv")
df.to_csv("agg_timeseries.csv.org")

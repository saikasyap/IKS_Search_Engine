from distutils.command.config import config
import json
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import math
from src.database import ElasticTransformers
import pandas as pd
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, helpers
import os
from urllib.parse import quote_plus as urlquote
from datetime import datetime
bert_embedder = SentenceTransformer("./pretrained_bert")

def embed_wrapper(ls):
    """
    Helper function which simplifies the embedding call and helps lading data into elastic easier
    """
    results=bert_embedder.encode(ls, convert_to_tensor=True)
    results = [r.tolist() for r in results]
    return results


def elastic_create_index(url,index_name,column_embed, df):
    et=ElasticTransformers(url=url,index_name=index_name)
    et.ping()
    et.create_index_spec(text_fields=df.columns.tolist(),dense_fields=[column_embed[0]+'_embedding', column_embed[1]+'_embedding'],dense_fields_dim=768)
    et.create_index()
    df.to_csv("./data/hrk_search.csv",index=False)
    et.write_large_csv('./data/hrk_search.csv',chunksize=3000,embedder=embed_wrapper,field_to_embed=column_embed)
    print("creating new indexing done")

# Function to delete elastic search index.. 
def delete_index(url,index_name):
    es= Elasticsearch(url)
    es.indices.delete(index=index_name, ignore=[400, 404])
    print("elastic Search Index deleted")



def create_data(config_path):

    with open(config_path, "r") as fp:
        config = json.load(fp)

    engine = create_engine(f'mysql+pymysql://{config["test_user"]}:{config["test_password"]}@{config["test_host"]}/{config["test_database"]}')

    #Transactional data - HRK
    trans_query = "select product_id, count(*) product_count from transaction_details group by product_id"
    trans = pd.read_sql(trans_query, engine)

    # Product Feed - HRK
    product_query  = "select product_id, title, sku as hotdeal_id, availability,purchasability,publishers as publisher, developers as developer, price, game_id, region from store_catalogue"
    product = pd.read_sql(product_query, engine)
    product["purchasability"].fillna(1.0, inplace = True)
    product = product.merge(trans, on=["product_id"], how = 'left')

    # Get the list of game_ids for meta data...
    hrk_game_nan =product["game_id"].unique().tolist()
    hrk_games = [item for item in hrk_game_nan if not(math.isnan(item)) == True]

    conn_str = 'mysql+pymysql://%s:%s@%s/%s'  % (config["user"], urlquote(config["password"]), config["host"], config["database"])
    # Get Alternate Titles 
    sql_query = "select g.id as game_id, g.title, alt.title_type, alt.title as alt_title from games g inner join releases r on r.game_id=g.id inner join publications p on p.release_id=r.id inner join alternate_titles alt on alt.publication_id=p.id where g.id in {} and p.locality_id=1 and g.deleted_at is null and r.deleted_at is null and p.deleted_at is null and alt.deleted_at is null order by g.id;".format(tuple(hrk_games))
    games = pd.read_sql(sql_query, conn_str)
    games = games.drop_duplicates(subset = ["alt_title"])
    games = games[["game_id", "alt_title","title_type"]]
    games1 = games[games['title_type'].isin(["Abbreviation","Informal","Official","Store Page","Series"])].reset_index(drop = True)
    alt_titles1 = pd.merge(product,games1, on ="game_id",how='left')
    alt_titles1["title_type"].fillna('No value', inplace = True)
    alt_titles = alt_titles1[alt_titles1['title_type'].isin(["Abbreviation","Informal","Official","Store Page","Series", "No value"])].reset_index(drop = True)
    # Get Release dates from the system..
    n_sql = "select g.id as game_id, g.title, p.release_date_time from games g inner join releases r on r.game_id=g.id inner join publications p on p.release_id=r.id where g.id in {} and p.locality_id=16 and g.deleted_at is null and p.updated_at  and r.deleted_at is null and p.deleted_at is null order by g.id;".format(tuple(hrk_games))
    pubs = pd.read_sql(n_sql, conn_str)

    pubs = pubs.drop_duplicates()


    pub =pubs.groupby('game_id').agg({'release_date_time':'min'})[['release_date_time']].reset_index()
    alt_titles = alt_titles.merge(pub, on=["game_id"], how = 'left')
    # Get Game Category from System
    game_category_query  = "select g.id as game_id, g.title, gt.name as category from games as g join game_types as gt on g.game_type_id=gt.id where g.id in {} and g.deleted_at is null;".format(tuple(hrk_games))
    game_category = pd.read_sql(game_category_query, conn_str)
    game_category = game_category.drop('title', axis =1)
    # Merging Data
    df = alt_titles.merge(game_category, on=["game_id"], how = 'left')
    df = df[df['product_id'].notna()]
    df['product_id'] = df['product_id'] .astype(float)
    df= df.rename(columns = {"category": "gop_category", 'product_count':'count', 'title': 'hrk_title','product_id':'sku'})
    df.drop_duplicates(inplace=True)
    df['count'] = df['count'].fillna(0)
    df['release_date_time'] = df['release_date_time'].fillna(0)
    df=df.fillna("No Value")
    df["region"] = df["region"].apply(lambda x: x.replace("Other", "Global"))
    df["region"] = df["region"].apply(lambda x: x.replace("No Value", "US"))
    df=df.drop(['game_id','title_type'], axis=1)

    df.drop_duplicates(inplace=True)
    print(len(df))
    print("Data has been generated")
    return df


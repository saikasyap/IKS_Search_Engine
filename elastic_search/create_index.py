from src.database import ElasticTransformers
import pandas as pd
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, helpers



bert_embedder = SentenceTransformer("./pretrained_bert")
url='http://localhost:9200'


# Read Search Master Data as CSV file.. 
df=pd.read_csv('Ramayan.csv')
#df.drop_duplicates(inplace=True)
# Few preprocessing steps for missing data
#df['count'] = df['count'].fillna(0)
#df['release_date_time'] = df['release_date_time'].fillna(0)
#df=df.fillna("No Value")
#The next line can be commented out 
#df = df.drop('Unnamed: 0', 1)


def embed_wrapper(ls):
    """
    Helper function which simplifies the embedding call and helps lading data into elastic easier
    """
    results=bert_embedder.encode(ls, convert_to_tensor=True)
    results = [r.tolist() for r in results]
    return results


def elastic_create_index(url,index_name,column_embed):
    et=ElasticTransformers(url=url,index_name=index_name)
    et.ping()
    et.create_index_spec(
        text_fields=df.columns.tolist())
    et.create_index()
    #df.to_csv("hrk_search.csv",index=False)
    et.write_large_csv('/home/kasyap/search/search_data.csv',
                  chunksize=3000,
                  embedder=None,
                  field_to_embed=None)

    #print(column_embed + " indexing done")

# Function to delete elastic search index.. 
def delete_index(url,index_name):
    es= Elasticsearch(url)
    es.indices.delete(index=index_name, ignore=[400, 404])
    print("elastic Search Index deleted")


# Create both hrk_title  & alt-title embedding in a single search Index ...  
#
elastic_create_index(url,'search', None)

#delete_index(url, 'gop_gametitle_latest')
#delete_index(url, 'service')
#delete_index(url, 'gop_alternate_title_latest')
#delete_index(url, 'test-index')
#delete_index(url, 'gop_editions_titles')
#delete_index(url, 'api')
#es= Elasticsearch(url)
#print(es.indices.get_alias("*"))

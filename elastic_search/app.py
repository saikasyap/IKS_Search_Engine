import warnings
from src.database import ElasticTransformers
from sentence_transformers import SentenceTransformer
import pandas as pd
from pandas.api.types import CategoricalDtype
from datetime import datetime, timedelta
import numpy as np
from aksharamukha import transliterate
from cltk.sentence.san import SanskritRegexSentenceTokenizer
import pycdsl
pd.set_option("display.max_rows", None, "display.max_columns", None)

import streamlit as st
#from st_aggrid import AgGrid
pd.set_option("display.max_rows", None, "display.max_columns", None)
#nltk.download('stopwords')

tokenizer = SanskritRegexSentenceTokenizer()

bert_embedder = SentenceTransformer("./pretrained_bert")
# similarity = Similarity("valhalla/distilbart-mnli-12-3")
url = "http://localhost:9200"
et = ElasticTransformers(url, index_name='search')


CDSL = pycdsl.CDSLCorpus()
CDSL.setup()
def embed_wrapper(ls):
    """
    Helper function which simplifies the embedding call and helps lading data into elastic easier
    """
    results = bert_embedder.encode(ls, convert_to_tensor=True)
    results = [r.tolist() for r in results]
    return results

# Converting Search Results to pandas DataFrame
def res_toDF(res):
    hits = res['hits']['hits']
    if len(hits) > 0:
        keys = list(hits[0]['_source'].keys())

        out = [[h['_score']] + [h['_source'][k] for k in keys] for h in hits]
        df = pd.DataFrame(out, columns=['_score'] + keys)
      
    else:
        df = pd.DataFrame([])
    return df



def basic_search(query,field):
    res_1 = et.search(query,field=field,type='match',embedder=None, size =1000)
    df1 = res_toDF(res_1)
    if not df1.empty:
        df1 = df1.sort_values(by='_score', ascending=False)
        df1 = df1.drop_duplicates()
    else:
        df1 = pd.DataFrame([])
    return df1


STYLE = """
<style>
img {
    max-width: 100%;
}
</style> """
# CSS to inject contained in a string
hide_dataframe_row_index = """
            <style>
            .row_heading.level0 {display:none}
            .blank {display:none}
            </style>
            """

# Inject CSS with Markdown
lst1 = []
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)
def main():
    """ Common ML Dataset Explorer """

    #from PIL import Image
    #image = Image.open('game.jpeg')
    #st.image(image, use_column_width=True)

    html_temp = """
	<div style="background-color:tomato;"><p style="color:white;font-size:40px;padding:9px">Indic Language Search Engine</p></div>
	"""
    st.markdown(html_temp, unsafe_allow_html=True)
    

    # Collect Input from user :
    query = str()
    query = str(st.text_input("Enter the Text you want to search(Press Enter once done)"))
    st.write("Look up in the dictionary:")
    results = CDSL["MW"].search(query)
    st.write(results)
    try:
        if len(query) > 0:
        # Call the function to extract the data. pass the topic and filename you want the data to be stored in.
            with st.spinner("Please wait, Search Results are being extracted"):
            #Akshara Mukha translation to 
                query1 = transliterate.process('autodetect', 'deva',  query, param="script_code")
                lst = basic_search(query1, 'text')
                tokenized_sent = tokenizer.tokenize(query1)
                if len(tokenized_sent) >1:
                    for i in tokenized_sent:
                        lst2 = basic_search(i, 'text')
    
                    lst.append(lst2, ignore_index = True)
                    lst.drop_duplicates(subset='text')

        

        #print(lst['Results'])

        st.dataframe(lst,1500)
        st.write ("Translated query :" , query1)

        st.success('Search results have been extracted !!!!')
    except:
        st.write("No results Found")
    if st.button("Exit"):
        st.balloons()


if __name__ == '__main__':
    main()

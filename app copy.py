import warnings
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from aksharamukha import transliterate
from cltk.sentence.san import SanskritRegexSentenceTokenizer
import pycdsl
pd.set_option("display.max_rows", None, "display.max_columns", None)
warnings.simplefilter("ignore")
import streamlit as st
#from st_aggrid import AgGrid
pd.set_option("display.max_rows", None, "display.max_columns", None)
#nltk.download('stopwords')

import warnings
warnings.simplefilter("ignore")
import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts = [
    'http://localhost:9200'
])




# CLTK Tokenizer

tokenizer = SanskritRegexSentenceTokenizer()


# similarity = Similarity("valhalla/distilbart-mnli-12-3")
url = "http://localhost:9200"
et = ElasticTransformers(url, index_name='search')


CDSL = pycdsl.CDSLCorpus()
CDSL.setup()

import streamlit as st


col1, col2, col3 = st.columns(3)

with col1:
    st.write(' ')

with col2:
    st.image("/home/kasyap/search/data/indic-search-logo.png", width=250 )

with col3:
    st.write(' ')


def searchElastic(query, pp=400):
    

    query_vector = model.infer([(str(query).split(), 0)])[0]

    script_query = {
        "script_score": {
            "query": {"match_all": {}},
            "script": {
                "source": "cosineSimilarity(params.query_vector, doc['title_vec']) + 1.0",
                "params": {"query_vector": query_vector}
            }
        }
    }

    response = es.search(
        index="search",
        body={
            "size": pp,
            "query": script_query,
            #"_source": {"includes": ["title", "body"]}
        }
    )

    return response


name = st.text_input("Enter Your query here", "")
if(st.button('Submit')):
	query = name.title()


st.markdown("#### Use the below dropdown to select number of results required")

number = st.selectbox("Number of results: ",
                     ['Top 10', 'Top 50', 'Top 100', 'All'])
 
pp = 10
# print the selected hobby
st.write("You wish to see ", number, " results")

if(number == 'Top 10'):
    pp = 10
    
elif(number == 'Top 50'):
    pp = 50
    
elif(number == 'Top 100'):
    pp = 100
    
else:
    pp = 500



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

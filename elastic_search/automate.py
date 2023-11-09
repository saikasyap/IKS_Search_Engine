import requests
import json
from data_pipeline import *
import os
from elasticsearch import Elasticsearch, helpers
#from dotenv import load_dotenv
def Fix_Index(url, oldIndexName, newIndexName, aliasName, isRemoveOldIndex):
    # Pre validation
    getIndices = url + "/_cat/indices"
    params = {'format': "json"}
    print("Pre validation started...")
    try:
        response = requests.get(url=getIndices, params=params).json()
        for indices in response:
            if indices['index'] == aliasName:
                exceptionMessage = ExceptionMessageCreator("Please check your aliasName and indexes, it should not "
                                                           "match with any index that present in your elastic search.")
                raise SystemExit(exceptionMessage)
    except requests.exceptions.RequestException as e:
        customMessage = "Pre validation failed\n"
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    # Adding alias to oldIndex, ElasticSearch will override the alias so no worry if it already present.
    print("Adding alias to index ", oldIndexName)
    try:
        addAliasUrl = url + "/" + oldIndexName + "/_alias/" + aliasName
        response = requests.put(url=addAliasUrl).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Alias added to index ", oldIndexName)
        else:
            customMessage = "Adding alias name to index fails, Please check your inputs that is proper or not\n"
            errorMessage = "ElasticSearch Response = {0}".format(str(response))
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Alias adding to index {0} failed\n".format(oldIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    

    # Index data migration
    print("Started data migration from index {0} to index {1}".format(oldIndexName, newIndexName))
    reIndexUrl = url + "/_reindex"
    reIndexBody = {"source": {"index": oldIndexName}, "dest": {"index": newIndexName}}
    print("Data migration request url is {0}\nRequest body is {1}".format(reIndexUrl, reIndexBody))
    try:
        response = requests.post(url=reIndexUrl, json=reIndexBody).json()
        if not response['failures']:
            print(
                "Data Migration Successfully completed from index {0} to index {1}".format(oldIndexName, newIndexName))
        else:
            customMessage = "Data Migration fail\n"
            errorMessage = str(response)
            errorResponse = customMessage + errorMessage
            rollBackOperations(url, newIndexName, errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Error occurs while migrating data from index {0} to index{1}\n".format(oldIndexName,
                                                                                                newIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        rollBackOperations(url, newIndexName, errorResponse)

    # Exchanging aliases
    print("Started exchanging aliases")
    aliasReplaceUrl = url + "/_aliases"
    aliasReplaceBody = {
        "actions": [
            {"remove": {"index": oldIndexName, "alias": aliasName}},
            {"add": {"index": newIndexName, "alias": aliasName}}
        ]
    }
    print("Alias exchanging request url is {0}, and request body is {1}".format(aliasReplaceUrl, aliasReplaceBody))
    try:
        response = requests.post(url=aliasReplaceUrl, json=aliasReplaceBody).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Exchanging aliases successfully completed")
        else:
            customMessage = "Alias exchanging failed\n"
            errorMessage = str(response)
            errorResponse = customMessage + errorMessage
            rollBackOperations(url, newIndexName, errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Error occurs while adding alias to index {0} and removing alias from index " \
                        "{1}\n".format(oldIndexName, newIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        rollBackOperations(url, newIndexName, errorResponse)

    # Checking isRemoveOldIndex flag
    if isRemoveOldIndex:
        url = url + "/" + oldIndexName
        print("Deleting old index started with url -> {}".format(url))
        try:
            response = requests.delete(url=url).json()
            if 'acknowledged' in response and response['acknowledged']:
                print("Deleting old index {0} successfully completed".format(oldIndexName))
                print("All tasks are successfully completed.")
                print("Your application is now pointed to index {}".format(newIndexName))
            else:
                customMessage = "Error occurs while deleting oldIndex, please try to delete manually\n"
                errorMessage = str(response)
                errorResponse = customMessage + errorMessage
                raise SystemExit(errorResponse)
        except requests.exceptions.RequestException as e:
            customMessage = "Error occurs while deleting oldIndex, please try to delete manually\n"
            errorMessage = "Error Response = {0}".format(str(e))
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    else:
        print("All tasks are successfully completed.")
        print("Your application is now pointed to index {}".format(newIndexName))


def rollBackOperations(url, newIndexName, errorResponse):
    print("Rollback started..")
    rollBackUrl = url + "/" + newIndexName
    try:
        response = requests.delete(url=rollBackUrl).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Rollback successfully completed.")
            raise SystemExit(errorResponse)
        else:
            customMessage = "Rollback Failed, Please try to delete newIndex manually\n"
            errorMessage = str(response)
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Rollback Failed, Please try to delete newIndex manually\n"
        errorMessage = "Error Response = ", str(e)
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)


def ExceptionMessageCreator(Message):
    predecessor = "Custom Error Message = "
    successor = "\nAborting Script...."
    return predecessor + str(Message) + successor

def delete_index(url,index_name):
    es= Elasticsearch(url)
    es.indices.delete(index=index_name, ignore=[400, 404])
    print("elastic Search Index deleted")


if __name__ == "__main__":
    lst = ['search_v1','search_new']
    url='http://ec2-65-2-95-183.ap-south-1.compute.amazonaws.com:9200'
    config_path='config.json'
    
    #delete_index(url,'search_v1')
    alias = "gop_search"
    #Creating Index
    """
    es= Elasticsearch(url)
   
    df = create_data(config_path)
    #Creating Index
    newIndex = 'search_new'
    elastic_create_index(url, 'search_new', ['hrk_title','alt_title'],df)
    """
    es= Elasticsearch(url)
    print(es.indices.get_alias("*"))
    indices=list(es.indices.get_alias().keys())
    old_lst =  list(set(lst) & set(indices))
    newIndex = list(set(lst)-set(old_lst))[0]
    #newIndex = 'search_v1'

    oldIndex = old_lst[0]
    print("old Index :", oldIndex)
    print("new index :", newIndex)
   
    df = create_data(config_path)
   
    #Creating Index
    elastic_create_index(url, newIndex, ['hrk_title','alt_title'],df)
    # Want to remove old Index or not
    isRemoveOldIndex = True
    # To switch index using alias
   
    Fix_Index(url, oldIndex, newIndex, alias, isRemoveOldIndex)
    
    print(es.indices.get_alias("*"))
    






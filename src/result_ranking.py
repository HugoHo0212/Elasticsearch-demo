"""
Add the syn
"""

import data_loading as dl
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

def main():
    """
    The main function
    """

    es = Elasticsearch()
    ic = IndicesClient(es)
    dl.create_wikipedia_index(ic)
    dl.load_data(es)

    print("The top ranked title without synonym:", search_and_rank(es))
    add_synonyms_to_index(ic)
    print("The top ranked title with synonym:", search_and_rank(es))


def search_and_rank(es: Elasticsearch) -> str:
    """
    Rank the documents by the terms "BC", "WA" and "AB"
    in the document body.
    Return the **title** of the top result.

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    str
        The title of the top ranked document
    """

    # 
    q ={
        "query":{
            "bool":{
                "must":{
                    "match":{
                        "body":{
                            "query":"lake tour",
                            "operator": "or"
                        }
                    }
                },
                "must_not":{
                    "match_phrase":{
                        "body":{
                            "query":"Please improve this article if you can."
                        }
                    }
                },
                "should":[
                    {"match":{
                        "body":{
                            "query":"BC",
                            "boost": 2
                        }
                    }},
                    {"match":{
                        "body":{
                            "query":"AB",
                            "boost": 1
                        }
                    }},
                    {"match":{
                        "body":{
                            "query":"WA",
                            "boost": 1
                        }
                    }}

                ]
            }
        }   
    }
    res = es.search(index = "wikipedia", body = q)
    return res['hits']['hits'][0]['_source']['title']


def add_synonyms_to_index(ic: IndicesClient) -> None:
    """
    Modify the index setting, add synonym mappings for "BC" => "British Columbia",
    "WA" => "Washington" and "AB" => "Alberta"

    Parameters
    ----------
    ic : IndicesClient
        The client for control index settings in Elasticsearch
    
    Returns
    -------
    None
    """

    # 
    body = {
        "analysis":{
            "analyzer":{
                "my_analyzer":{
                    "filter":["lowercase",
                                "my_stops",
                                "my_synonyms"]
                }
            },
            "filter":{
                "my_synonyms":{
                    "type": "synonym",
                    "synonyms":["British Columbia, BC", "Alberta, AB", "Washington, WA"]
                }
            }
        }
    }

    ic.close(index = "wikipedia")
    ic.put_settings(body = body,index= "wikipedia")

    ic.open(index = "wikipedia")


if __name__ == "__main__":
    main()

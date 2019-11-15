"""
Issue a simple query
"""
from typing import Dict, Any
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
    print(f"There are {filter(es)['hits']['total']['value']} documents contains 'lake' or 'tour'")
    print(
        f"There are {search_without_improvement(es)['hits']['total']['value']} documents contains"
        " 'lake' or 'tour', but without the 'improvement required' sentense."
    )


def filter(es: Elasticsearch) -> Dict[str, Any]:
    """
    Issue a query to Elasticsearch, 
    return documents whose **body** only contains word "lake" **or** "tour".

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    Dict[str, Any]
        The raw query result from elasticsearch
    """
    # 
    q ={
        "query":{
            "match":{
                "body":{
                    "query":"lake tour",
                    "operator": "or"
                }
            }
        }
    }
    res = es.search(index = "wikipedia", body = q)
    return res

def search_without_improvement(es: Elasticsearch) -> Dict[str, Any]:
    """
    Issue a query to Elasticsearch, 
    return documents whose **body** only contains word "lake" **or** "tour",
    and not contains the sentense "Please improve this article if you can."

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    Dict[str, Any]
        The raw query result from elasticsearch
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
                }
            }
        }   
    }
    res = es.search(index = "wikipedia", body = q)
    return res


if __name__ == "__main__":
    main()

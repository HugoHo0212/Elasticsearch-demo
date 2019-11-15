from typing import Tuple
import tarfile
import os

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from pyquery import PyQuery as pq 


def load_data(es: Elasticsearch) -> None:
    """
    This function loads data from the tarball "wiki-small.tar.gz" 
    to the Elasticsearch cluster

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    None
    """

    # 
    tf = tarfile.open("wiki-small.tar.gz")
    count = 1
    for tarinfo in tf.getmembers():
        if(".html" in os.path.split(tarinfo.name)[1]):
            file = tf.extractfile(tarinfo)
            contents = file.read()
            tup = parse_html(contents)
            title = tup[0]
            body = tup[1]
            d = {
                "title": title,
                "body": body
            }
            es.index(index="wikipedia", id=count,body=d)
            count += 1
    tf.close()



def parse_html(html: str) -> Tuple[str, str]:
    """
    This function parses the html, strips the tags an return
    the title and the body of the html file.

    Parameters
    ----------
    html : str
        The HTML text

    Returns
    -------
    Tuple[str, str]
        A tuple of (title, body)
    """

    #
    doc = pq(html)
    title = doc("title").text()
    body = doc("body").text()
    return (title, body)

def create_wikipedia_index(ic: IndicesClient) -> None:
    """
    Add an index to Elasticsearch called 'wikipedia'

    Parameters
    ----------
    ic : IndicesClient
        The client to control Elasticsearch index settings

    Returns
    -------
    None
    """
    # 
    ic.create(
        index = "wikipedia",
        body = {
            "settings":{
                "analysis":{
                    "analyzer":{
                        "my_analyzer":{
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "my_stops"
                            ]
                        }
                    },
                    "filter":{
                        "my_stops":{
                            "type": "stop",
                            "stopwords_path":"stopwords.txt"
                        }
                    }
                }
            },
            'mappings':{       
                'properties':{
                    'body':{
                        'type': 'text',
                        'analyzer': 'my_analyzer'
                    }
                } 
            }
        }
    )

"""
Create index and load data
"""

import assignment4 as a4
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


def main():
    """
    The main function
    """

    es = Elasticsearch()
    ic = IndicesClient(es)
    a4.create_wikipedia_index(ic)
    a4.load_data(es)

    print(count_documents(es), "documents loaded")


def count_documents(es: Elasticsearch) -> int:
    """
    Count how many documents loaded in the wikipedia index

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client

    Returns
    -------
    int
        The documents count.
    """
    ### 
    return es.count(index="wikipedia")['count']


if __name__ == "__main__":
    main()

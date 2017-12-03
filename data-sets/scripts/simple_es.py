#!/usr/bin/python2.7
"""
Python Version: Python 2.7

Simple Python script for reading, writing, and deleting documents from elasticsearch.
"""
from elasticsearch import Elasticsearch
import argparse
import json

debug = False
es = Elasticsearch([{'host':'localhost'}])

def debug_print(*argv):
    """
    Debug printing
    :param argv: the content to be printed
    :return:
    """
    if debug == True:
        print argv

def single_doc_write(list_docs, index='test_index', type='test_type'):
    """
    Writes documents one by one to elasticsearch
    :param list_docs: accepts a list and itterates over it passing each document to es
    :param index:
    :param type:
    :return:
    """
    es.indices.create(index=index, ignore=400)

    for doc in list_docs:
        if 'id' in doc:
            res = es.index(index=index, doc_type=type, id=doc['id'], body=doc)
        else:
            res = es.index(index=index, doc_type=type, body=doc)
        debug_print(res)

def get_document(list_ids, index='test_index', type='test_type'):
    """
    :param list_ids: list of document ids or list of documents containing id
    :param index:
    :param type:
    :return: a list of documents that exist based on ids provided
    """
    ret = []
    for doc in list_ids:
        if isinstance(doc, dict):
            res = es.get(index=index, doc_type=type, id=doc['id'])
        else:
            res = es.get(index=index, doc_type=type, id=id)
        ret.append(res['_source'])
        debug_print(res['_source'])
    return ret

def get_documents(index='test_index', type='test_type'):
    """
    General search for all documents in es
    :param index:
    :param type:
    :return: a list of hits
    """
    res = es.search(index=index, doc_type=type, body={"query": {"match_all": {}}})
    debug_print('Got {} hits'.format(res['hits']['total']))
    if debug == True:
        for hit in res['hits']['hits']:
            print hit
    return res['hits']['hits']

def delete_index(index='test_index'):
    """
    Deletes index
    :param index:
    :return:
    """
    es.indices.delete(index=index, ignore=[400, 404])

def setup_argparse(parser=None):
    if parser == None:
        parser = argparse.ArgumentParser(description='Simple Python script for reading, writing, and deleting '
                                                     'documents from elasticsearch.')
    parser.add_argument('-w', '--write', dest='write', action='store_true', default=False, help='writes documents to es')
    parser.add_argument('-d', '--delete', dest='delete', action='store_true', default=False, help='deletes the index specified')
    parser.add_argument('-s', '--search', dest='search', action='store_true', default=False, help='simple search retireves document '
                                                                                     'relating to index and type')
    parser.add_argument('-g', '--get', dest='get', action='store_true', default=False, help='gets the following documents from es')
    parser.add_argument('-i', '--index', dest='index', help='sets index name otherwise uses default')
    parser.add_argument('-t', '--type', dest='type', help='sets type name otherwise uses default')
    parser.add_argument('-p', '--path', dest='path', help='specifies the location of the json file (required for '
                                                          'majority of options)')
    parser.add_argument('--debug', dest='debug', action='store_true', default=False, help='enables debug mode (adds more print statments)')
    return parser

if __name__=='__main__':
    parser = setup_argparse()
    args = parser.parse_args()

    documents = dict()
    index = 'test_index'
    type = 'test_type'
    debug = args.debug

    if args.path is not None:
        f = open(args.path, 'r')
        documents = json.loads(''.join(f.readlines()))
    if args.index is not None:
        index = args.index
    if args.type is not None:
        type = args.type
    if args.write is True:
        single_doc_write(documents, index=index, type=type)
    if args.delete is True:
        delete_index(index=index)
    if args.search is True:
        get_documents(index=index, type=type)
    if args.get is True:
        get_document(documents, index=index, type=type)
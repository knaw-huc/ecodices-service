from elasticsearch import Elasticsearch
import math

class Index:
    def __init__(self, config):
        self.config = config
        self.es = Elasticsearch(hosts=[{"host": "ecodices_es"}], retry_on_timeout=True)
        self.client = Elasticsearch(hosts=[{"host": "ecodices_es"}], retry_on_timeout=True)

    def no_case(self, str_in):
        str = str_in.strip()
        ret_str = ""
        if (str != ""):
            for char in str:
                ret_str = ret_str + "[" + char.upper() + char.lower() + "]"
        return ret_str + ".*"


    def get_facet(self, field, amount):
        ret_array = []
        response = self.client.search(
            index="manuscripts",
            body=
                {
                    "size": 0,
                    "aggs": {
                        "names": {
                            "terms": {
                                "field": field,
                                "size": amount,
                                "order": {
                                    "_count": "desc"
                                }
                            },
                            "aggs": {
                                "byHash": {
                                    "terms": {
                                        "field": "hash"
                                    }
                                }
                            }
                        }
                    }
                }
        )
        for hits in response["aggregations"]["names"]["buckets"]:
            buffer = {"key": hits["key"], "doc_count": hits["doc_count"]}
            ret_array.append(buffer)
        return ret_array

    def get_filter_facet(self, field, amount, facet_filter):
        print(facet_filter)
        ret_array = []
        response = self.client.search(
            index="manuscripts",
            body=
            {
                "query": {
                    "regexp": {
                        field : self.no_case(facet_filter)
                    }
                },
                "size": 0,
                "aggs": {
                    "names": {
                        "terms": {
                            "field": field,
                            "size": amount,
                            "order": {
                                "_count": "desc"
                            }
                        }
                    }
                }
            }
        )
        for hits in response["aggregations"]["names"]["buckets"]:
            buffer = {"key": hits["key"], "doc_count": hits["doc_count"]}
            ret_array.append(buffer)
        return ret_array



    def browse(self, page, length, orderFieldName, searchvalues):
        int_page = int(page)
        start = (int_page -1) * length
        matches = []

        if searchvalues == "none":
            response = self.client.search(
                index="manuscripts",
                body={ "query": {
                    "match_all": {}},
                    "size": length,
                    "from": start,
                    "_source": ["xml", "title", "origPlace", "origDate"],
                    "sort": [
                        { orderFieldName: {"order":"asc"}}
                    ]
                }
            )
        else:
            for item in searchvalues:
                for value in item["values"]:
                    if item["field"] == "FREE_TEXT":
                        matches.append({"multi_match": {"query": value, "fields": ["*"]}})
                    else:
                        matches.append({"match": {item["field"] + ".keyword": value}})
            response = self.client.search(
                index="manuscripts",
                body={ "query": {
                    "bool": {
                        "must": matches
                    }},
                    "size": length,
                    "from": start,
                    "_source": ["xml", "title", "origPlace", "origDate", "location", "shelfmark", "itemAuthor", "itemTitle", "layout", "measure", "summary", "textLang"],
                    "sort": [
                        { orderFieldName: {"order":"asc"}}
                    ]
                }
            )

        ret_array = {"amount" : response["hits"]["total"]["value"], "pages": math.ceil(response["hits"]["total"]["value"] / length) ,"items": []}
        for item in response["hits"]["hits"]:
            ret_array["items"].append(item["_source"])
        return ret_array








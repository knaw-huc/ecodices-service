from elasticsearch import Elasticsearch
import math
import xmltodict
from benedict import benedict
import operator
import glob, os, codecs
import unicodedata

class Index:
    def __init__(self, config):
        self.config = config
        #self.client = Elasticsearch(hosts=[{"host": "ecodices_es"}], retry_on_timeout=True)
        self.client = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}], retry_on_timeout=True)
        self.cmdi_source = "/Users/robzeeman/Desktop/Werkmap/ecodices/xml_selectie/"
        #self.cmdi_source = "/xml/"
        self.languages = {"ara": "Arabic",
                          "eng": "English",
                          "fra": "French",
                          "fry": "Frisian",
                          "gla": "Gaelic",
                          "deu": "German",
                          "grc": "Ancient Greek",
                          "heb": "Hebrew",
                          "isl": "Icelandic",
                          "gle": "Irish",
                          "ita": "Italian",
                          "lat": "Latin",
                          "yid": "Yiddish",
                          "gml": "Middle Low German",
                          "mis": "Uncoded languages",
                          "nld": "Dutch",
                          "oci": "Occitan (post 1500)",
                          "pro": "Old Occitan (to 1500) Old Provençal (to 1500)",
                          "spa": "Spanish",
                          "und": "Undetermined"}

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
            index="manuscript",
            body=
                {
                    "size": 0,
                    "aggs": {
                        "names": {
                            "terms": {
                                "field": field,
                                "size": amount,
                                "order": {
                                    "_key": "asc"
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

    def get_shelfmark_facet(self, collection, amount):
        ret_array = []
        response = self.client.search(
            index="manuscript",
            body=
            {
                "query": {"bool": {"must": [
                    {"match": {
                        "collection.keyword": collection
                    }}
                ]}},
                "size": 0,
                "aggs": {
                    "names": {
                        "terms": {
                            "field": "shelfmark.keyword",
                            "size": amount,
                            "order": {
                                "_key": "asc"
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
        ret_array = []
        response = self.client.search(
            index="manuscript",
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
                index="manuscript",
                body={ "query": {
                    "match_all": {}},
                    "size": length,
                    "from": start,
                    "_source": ["xml", "title", "place", "origin", "support", "origDate", "location", "shelfmark", "settlement", "itemAuthor", "itemTitle", "layout", "measure", "summary", "textLang"],
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
                index="manuscript",
                body={ "query": {
                    "bool": {
                        "must": matches
                    }},
                    "size": length,
                    "from": start,
                    "_source": ["xml", "title", "place", "origin", "support", "origDate", "location", "shelfmark", "settlement", "itemAuthor", "itemTitle", "layout", "measure", "summary", "textLang"],
                    "sort": [
                        { orderFieldName: {"order":"asc"}}
                    ]
                }
            )

        ret_array = {"amount" : response["hits"]["total"]["value"], "pages": math.ceil(response["hits"]["total"]["value"] / length) ,"items": []}
        for item in response["hits"]["hits"]:
            tmp_arr = item["_source"]
            tmp_arr["_id"] = item["_id"]
            ret_array["items"].append(tmp_arr)
        return ret_array

    def manuscript(self, id):
        response = self.client.search(
            index="manuscript",
            body={"query": {
                "terms": {
                    "_id": [id]
                }
            },
                "_source": ["summary","binding","language", "support", "origin", "tempDate","collection","title","type","origDate","settlement","material","xml","place","decoration","shelfmark", "licence", "measure"]
            }
        )
        if response["hits"]["total"]["value"] == 1:
            return response["hits"]["hits"][0]["_source"]
        else:
            return []

    def full_description(self, id):
        namespaces = {'http://www.clarin.eu/cmd/': None}
        with open(self.cmdi_source + id) as f:
            return self.parse_manuscript(xmltodict.parse(f.read(), process_namespaces=True, namespaces=namespaces))

    def delete_from_index(self, xml):
        response = self.client.delete_by_query(
            index="manuscript",
            body={
                "query": {
                    "term": {
                        "xml": xml
                    }
                },
                "max_doc": 1
            }
        )
        return response


    def parse_manuscript(self, json_doc):
        retDoc = []
        doc = json_doc["CMD"]["Components"]["eCodices"]
        d = benedict(doc)
        self.pack_item("settlement", "Settlement", "Source.Identifier.Settlement.settlement", retDoc, d)
        self.pack_item("repository", "Repository", "Source.Identifier.Repository.repository", retDoc, d)
        self.pack_item("idno", "Identifier", "Source.Identifier.Idno.idno", retDoc, d)
        self.pack_item("idno", "Alt. identifier", "Source.Identifier.AltIdentifier.Idno.idno", retDoc, d)
        self.add_head(retDoc, d)
        self.add_contents(retDoc, d)
        self.add_phys_desc(retDoc, d)
        self.add_history(retDoc, d)
        self.add_publication(retDoc, d)
        return retDoc


    def add_contents(self, retDoc, doc):
        self.pack_line("Contents", retDoc)
        self.pack_item("summary", "Summary", "Source.Contents.Summary.summary", retDoc, doc)
        self.pack_language("textLang", "Language", "Source.Contents.textLang.textLang", retDoc, doc)
        if "Source.Contents.Item" in doc:
            self.add_items(retDoc, doc)


    def add_items(self, retDoc, doc):
        if isinstance(doc["Source.Contents.Item"], list):
            self.sortItemList(doc["Source.Contents.Item"])
            index = 0
            for item in doc["Source.Contents.Item"]:
                self.pack_line("Items", retDoc)
                self.add_item(index, "Source.Contents.Item[" + str(index) + "]" , retDoc, doc)
                index = index + 1
        else:
            self.pack_line("Items", retDoc)
            self.add_item(0, "Source.Contents.Item", retDoc, doc)

    def sortItemList(self, list):
        for item in list:
            if not 'itemOrder' in item:
                item["itemOrder"] = 0
        list.sort(key=operator.itemgetter('itemOrder'))
        return



    def add_item(self, index, path, retDoc, doc):
        if path in doc:
            retDoc.append({"field": str(index), "label": "Item " + str(index + 1), "value": ""})
            self.pack_item_block(path, retDoc, doc)

    def pack_item_block(self, path, retDoc, doc):
        self.pack_item("title", "༓ Title", path + ".Title.title", retDoc, doc)
        self.pack_item("title_ref", "༓ Title ref.", path + ".Title.title_ref", retDoc, doc)
        self.pack_item("author", "༓ Author", path + ".Author.author", retDoc, doc)
        self.pack_item("author_ref", "༓ Author ref.", path + ".Author.ref", retDoc, doc)
        self.pack_item("rubric", "༓ Rubric", path + ".rubric", retDoc, doc)
        self.pack_item("finalRubric", "༓ Final rubric", path + ".finalRubric", retDoc, doc)
        self.pack_item("colophon", "༓ Colophon", path + ".colophon", retDoc, doc)
        self.pack_item("locus", "༓ Locus", path + ".Locus.locus", retDoc, doc)
        self.pack_item("filiation", "༓ Filiation", path + ".Filiation.filiation", retDoc, doc)
        self.pack_item("incipit", "༓ Incipit", path + ".Incipit.incipit", retDoc, doc)
        self.pack_item("locus", "༓ Incipit locus", path + ".Incipit.Locus.locus", retDoc, doc)
        self.pack_language("mainLang", "༓ Incipit language", path + ".Incipit.mainLang", retDoc, doc)
        self.pack_item("explicit", "༓ Explicit", path + ".Explicit.explicit", retDoc, doc)
        self.pack_item("locus", "༓ Explicit locus", path + ".Explicit.Locus.locus", retDoc, doc)
        self.pack_language("mainLang", "༓ Explicit language", path + ".Explicit.mainLang", retDoc, doc)
        self.pack_item("note", "༓ Note", path + ".Note.note", retDoc, doc)


    def add_phys_desc(self, retDoc, doc):
        self.pack_line("PhysDesc", retDoc)
        self.pack_item("additions", "Additions", "Source.PhysDesc.additions", retDoc, doc)
        self.pack_item("accMat", "Acc. material", "Source.PhysDesc.accMat", retDoc, doc)
        self.pack_item("musicNotation", "Music notation", "Source.PhysDesc.musicNotation", retDoc, doc)
        self.add_object_desc(retDoc, doc)
        self.add_hand_desc(retDoc, doc)
        self.add_deco_desc(retDoc, doc)
        self.add_binding_desc(retDoc, doc)



    def add_object_desc(self, retDoc, doc):
        basePath = "Source.PhysDesc.ObjectDesc"
        if basePath in doc:
            self.pack_item("form", "Document type", basePath + ".form", retDoc, doc)
            self.add_support_desc(basePath, retDoc, doc)
            self.add_layout_desc(basePath, retDoc, doc)


    def add_support_desc(self, path, retDoc, doc):

        self.pack_item("condition", "Condition", path+ ".SupportDesc.condition", retDoc, doc)
        self.pack_item("foliation", "Foliation", path + ".SupportDesc.foliation", retDoc, doc)
        #self.pack_item("material", "Material", path + ".SupportDesc.Material.material", retDoc, doc)
        self.pack_item("support", "Material", path + ".SupportDesc.Support.support", retDoc, doc)
        self.pack_item("type", "Type", path + ".Extend.MeasureLeavesCount.type", retDoc, doc)
        self.pack_item("measure", "Measure", path + ".Extend.MeasureLeavesCount.measure", retDoc, doc)
        self.pack_item("type", "Type", path + "Extend.MeasurePageDimensions.type", retDoc, doc)
        self.pack_item("width", "Width", path + "Extend.MeasurePageDimensions.width", retDoc, doc)
        self.pack_item("heigth", "Heigth", path + "Extend.MeasurePageDimensions.height", retDoc, doc)
        if path + ".Extend.Measure" in doc:
            if isinstance(path + ".Extend.Measure", list):
                for item, index in doc[path + ".Extend.Measure"]:
                    self.pack_item("type", "Type", path + ".Extend.Measure[" + str(index) + "].type", retDoc, doc)
                    self.pack_item("measure", "Measure", path + ".Extend.Measure[" + str(index) + "].measure", retDoc, doc)
            else:
                self.pack_item("type", "Type", path + ".Extend.Measure.type", retDoc, doc)
                self.pack_item("measure", "Measure", path + ".Extend.Measure.measure", retDoc, doc)

        self.pack_item("collation", "Collation", path + ".SupportDesc.Collation.collation", retDoc, doc)
        self.pack_item("locus", "Locus", path + ".SupportDesc.Collation.Locus.locus", retDoc, doc)

    def add_layout_desc(self, path, retDoc, doc):
        self.pack_item("columns", "Columns", path + ".layoutDesc.Layout.Columns.columns", retDoc, doc)
        self.pack_item("writtenLines", "Written lines", path + ".layoutDesc.Layout.WrittenLines.writtenLines", retDoc, doc)
        self.pack_item("measure", "Measure", path + ".layoutDesc.Layout.Measure.measure", retDoc, doc)
        self.pack_item("width", "Width", path + ".layoutDesc.Layout.MeasuretextBlock.width", retDoc, doc)
        self.pack_item("height", "Height", path + ".layoutDesc.Layout.MeasuretextBlock.height", retDoc, doc)



    def add_hand_desc(self, retDoc, doc):
        if "Source.PhysDesc.HandDesc" in doc:
            self.pack_item("hands", "Hands", "Source.PhysDesc.HandDesc.hands", retDoc, doc)
            self.pack_item("summary", "Summary", "Source.PhysDesc.HandDesc.summary", retDoc, doc)
            if "Source.PhysDesc.HandDesc.HandNote" in doc:
                if isinstance(doc["Source.PhysDesc.HandDesc.HandNote"], list):
                    index = 0
                    for item in doc["Source.PhysDesc.HandDesc.HandNote"]:
                        self.pack_hand_note("Source.PhysDesc.HandDesc.HandNote[" + str(index) + "]", retDoc, doc)
                        index = index + 1
                else:
                    self.pack_hand_note("Source.PhysDesc.HandDesc.HandNote", retDoc, doc)

    def pack_hand_note(self, path, retDoc, doc):
        self.pack_item("script", "Script", path + ".script", retDoc, doc)
        self.pack_item("handNote", "Hand note", path + ".handNote", retDoc, doc)
        self.pack_item("locus", "Locus", path + "Locus.locus", retDoc, doc)

    def add_deco_desc(self, retDoc, doc):
        if "Source.PhysDesc.DecoDesc.DecoNote" in doc:
            if isinstance(doc["Source.PhysDesc.DecoDesc.DecoNote"], list):
                index = 0
                for item in doc["Source.PhysDesc.DecoDesc.DecoNote"]:
                    self.pack_deco_desc("Source.PhysDesc.DecoDesc.DecoNote[" + str(index) + "]", retDoc, doc)
                    index = index + 1
            else:
                self.pack_deco_desc("Source.PhysDesc.DecoDesc.DecoNote", retDoc, doc)


    def add_binding_desc(self, retDoc, doc):
        if "Source.PhysDesc.bindingDesc.Binding" in doc:
            if isinstance(doc["Source.PhysDesc.bindingDesc.Binding"], list):
                index = 0
                for item in doc["Source.PhysDesc.bindingDesc.Binding"]:
                    self.pack_binding("Source.PhysDesc.bindingDesc.Binding[" + str(index) + "]", retDoc, doc)
            else:
                self.pack_binding("Source.PhysDesc.bindingDesc.Binding", retDoc, doc)

    def pack_deco_desc(self, path, retDoc, doc):
        self.pack_item("decoNote", "Decoration", path + ".decoNote", retDoc, doc)
        self.pack_item("locus", "Locus", path + ".Locus.locus", retDoc, doc)

    def pack_binding(self, path, retDoc, doc):
        self.pack_item("binding", "Binding", path + ".binding", retDoc, doc)
        self.pack_item("date", "Date", path + ".date", retDoc, doc)
        self.pack_item("notBefore", "Not before", path + ".notBefore", retDoc, doc)
        self.pack_item("notAfter", "Not after", path + ".notAfter", retDoc, doc)

    def add_head(self, retDoc, d):
        self.pack_line("Head", retDoc)
        if isinstance(d["Source.Head.OrigPlace"], list):
            self.pack_list("origPlace", "Place of origin", "Source.Head.OrigPlace", retDoc, d)
        else:
            self.pack_item("origPlace", "Place of origin", "Source.Head.OrigPlace.origPlace", retDoc, d)
            self.pack_item("origPlace", "Place of origin", "Source.Head.OrigPlace.note", retDoc, d)
        lst = [{"field": "origDate", "label": "Date"}]
        if isinstance(d["Source.Head.OrigDate"], list):
            self.pack_extended_list(lst, "Source.Head.OrigDate", retDoc, d)
        else:
            self.pack_extended_item(lst, "Source.Head.OrigDate", retDoc, d)
        self.pack_item('note', "Note",  "Source.Head.Note.note", retDoc, d)

    def add_publication(self, retDoc, doc):
        # Dummy bibliografie
        self.pack_line("Bibliography", retDoc)
        retDoc.append({"field": "dummyBiblio", "label": "Bibliography", "value": ""})
        self.pack_line("Publication", retDoc)
        self.pack_item("title", "Title", "Title.title", retDoc, doc)
        self.pack_item("creator", "Creator", "creator", retDoc, doc)
        self.pack_item("publisher", "Publisher", "Publication.publisher", retDoc, doc)
        self.pack_item("status", "Status", "Publication.Availability.status", retDoc, doc)
        self.pack_item("licence", "Licence", "Publication.Availability.licence", retDoc, doc)
        self.pack_item("licenceTarget", "Licence target", "Publication.Availability.licenceTarget", retDoc, doc)

    def add_history(self, retDoc, doc):
        self.pack_line("History", retDoc)
        if "Source.History.Origin.origin" in doc:
            self.pack_item("origin", "Origin", "Source.History.Origin.origin", retDoc, doc)
        if "Source.History.Provenance" in doc:
            if isinstance(doc["Source.History.Provenance"], list):
                self.pack_list("provenance", "Provenance", "Source.History.Provenance", retDoc, doc)
            else:
                self.pack_item("provenance", "Provenance", "Source.History.Provenance.provenance", retDoc, doc)
        self.pack_item("acquisition", "Acquisition", "Source.History.Acquisition.acquisition", retDoc, doc)
        if "Source.History.Acquisition.PersName" in doc:
            if isinstance(doc["Source.History.Acquisition.PersName"], list):
                self.pack_list("persName", "Person name", "Source.History.Acquisition.PersName", retDoc, doc)
            else:
                self.pack_item("persName", "Person name", "Source.History.Acquisition.PersName.persName", retDoc, doc)


    def pack_list(self, field, label, path, retDoc, doc):
        for item in doc[path]:
            if field in item:
                if isinstance(item[field], list):
                    for el in item[field]:
                        retDoc.append({"field": field, "label": label, "value": el})
                else:
                    retDoc.append({"field": field, "label": label, "value": item[field]})

    def pack_extended_list(self, lst, path, retDoc, doc):
        for item, index in doc[path]:
            self.pack_extended_item(lst, path + "[" + str(index) + "]", retDoc, doc)

    def pack_extended_item(self,  lst, path, retDoc, doc):
        for pair in lst:
            self.pack_item(pair["field"], pair["label"], path + '.' + pair["field"], retDoc, doc)

    def pack_item(self, field, label, path, retDoc, doc):
        if path in doc:
            if isinstance(doc[path], list):
                for item in doc[path]:
                    retDoc.append({"field": field, "label": label, "value": item})
            else:
                if field == 'creator':
                    retDoc.append({"field": field, "label": label, "value": "MMDC, " + doc[path]})
                else:
                    retDoc.append({"field": field, "label": label, "value": doc[path]})
        return

    def pack_language(self, field, label, path, retDoc, doc):
        if path in doc:
            if isinstance(doc[path], list):
                for item in doc[path]:
                    retDoc.append({"field": field, "label": label, "value": self.languages[item]})
            else:
                retDoc.append({"field": field, "label": label, "value": self.languages[doc[path]]})
        return


    def pack_line(self,  label, retDoc):
        retDoc.append({"field": "line", "label": label, "value": ""})








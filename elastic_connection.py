from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import csv
import json


class ElasticConnection:
    def __init__(self):
        self.user = 'elastic'
        self.password = 'changeme'
        self.index = "monster-01"
        self.elastic_client = Elasticsearch(
            host='localhost',
            http_auth=(self.user, self.password),
            scheme="http",
            port=9200,
            connection_class=RequestsHttpConnection
        )

    def read_main_log(self):
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": "2022-02-07T08:00:00.000Z",
                                    "lte": "2022-02-07T13:49:00.000Z"
                                }
                            }
                        },
                        {
                            "match": {
                                "severity": "info"
                            }
                        },
                        {
                            "regexp": {
                                "user_agent.keyword": ".{5,9}-server [0-9]{2,4}"
                            }
                        },
                        # {
                        #     "match": {
                        #         "user_agent": {
                        #             "query": "object-server container-server account-server proxy-server",
                        #             "operator": "or"
                        #         }
                        #     }
                        # },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase": {
                                            "programname": "object-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "account-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "proxy-server"
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            }
        }

        records = self.elastic_client.search(index=self.index, body=body)
        # attribute_names = records['hits']['hits'][0]['_source'].keys()
        attribute_names = ["@timestamp", "programname", "severity", "message"]

        index_reader = helpers.scan(
            client=self.elastic_client,
            scroll='2m',
            query=body,
            index=self.index)

        # make_row = lambda record, key: record[key] if key in record.keys() else ''
        # with open('LogDB_main.csv', 'w', newline='') as csvfile:
        #     writer = csv.DictWriter(csvfile, fieldnames=attribute_names)
        #     writer.writeheader()
        #     i = 0
        #     for record in index_reader:
        #         new_row = {key: make_row(record['_source'], key) for key in attribute_names}
        #         writer.writerow(new_row)
        #         i += 1
        #         # if i == 10:
        #         #     break
        # pass
        with open('LogDB_main.json', 'w', encoding='utf-8') as jsonfile:
            for record in index_reader:
                json.dump(record['_source'], jsonfile, ensure_ascii=False, indent=4)
        pass

    def read_all_log(self):
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": "2022-02-07T08:00:00.000Z",
                                    "lte": "2022-02-07T13:49:00.000Z"
                                }
                            }
                        },
                        {
                            "match": {
                                "severity": "info"
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match_phrase": {
                                            "programname": "object-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "account-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "proxy-server"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-auditor"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "object-auditor"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-replicator"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "object-updater"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "object-replicator"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-sync"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "account-auditor"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "container-updater"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "programname": "account-replicator"
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            }
        }

        records = self.elastic_client.search(index=self.index, body=body)
        # attribute_names = records['hits']['hits'][0]['_source'].keys()

        index_reader = helpers.scan(
            client=self.elastic_client,
            scroll='2m',
            query=body,
            index=self.index)

        with open('LogDB_all.json', 'w', encoding='utf-8') as jsonfile:
            for record in index_reader:
                json.dump(record['_source'], jsonfile, ensure_ascii=False, indent=4)
        pass

        # Another way to get all the records of an index

        # body2 = {
        #     "query": {
        #         "term": {"user": self.user}
        #     }
        # }
        #
        # res = self.elastic_client.count(index=self.index, body=body2)
        # size = res['count']
        #
        # body = {"size": 10,
        #         "query": {
        #             "term": {
        #                 "user": self.user
        #             }
        #         },
        #         "sort": [
        #             {"date": "asc"},
        #             {"_uid": "desc"}
        #         ]
        #         }
        #
        # result = self.elastic_client.search(index=self.index, body=body)
        # bookmark = [result['hits']['hits'][-1]['sort'][0], str(result['hits']['hits'][-1]['sort'][1])]
        #
        # body1 = {"size": 10,
        #          "query": {
        #              "term": {
        #                  "user": self.user
        #              }
        #          },
        #          "search_after": bookmark,
        #          "sort": [
        #              {"date": "asc"},
        #              {"_uid": "desc"}
        #          ]
        #          }
        #
        # while len(result['hits']['hits']) < size:
        #     res = self.elastic_client.search(index=self.index, body=body1)
        #     for el in res['hits']['hits']:
        #         result['hits']['hits'].append(el)
        #     bookmark = [res['hits']['hits'][-1]['sort'][0], str(result['hits']['hits'][-1]['sort'][1])]
        #     body1 = {"size": 10,
        #              "query": {
        #                  "term": {
        #                      "user": self.user
        #                  }
        #              },
        #              "search_after": bookmark,
        #              "sort": [
        #                  {"date": "asc"},
        #                  {"_uid": "desc"}
        #              ]
        #              }

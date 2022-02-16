from elastic_connection import ElasticConnection

if __name__ == '__main__':
    elastic_client = ElasticConnection()
    # elastic_client.read_all_log()
    elastic_client.read_main_log()


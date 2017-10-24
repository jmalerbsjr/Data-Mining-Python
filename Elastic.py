import requests
import json
import datetime

##### GLOBAL PARAMETERS ##### 
elasticEndpoint = 'http://localhost:9200'
#############################

def LocalEndpoint():
    
    return elasticEndpoint


def WriteData(elasticEndpoint,elsIndexName, elsIndexType, data, docId):
    '''Write documents to Elastic cluster.'''
    headers = {'Content-elsIndexType': 'application/json'}
    url = '{0}/{1}/{2}/{3}'.format(elasticEndpoint, elsIndexName, elsIndexType, docId)
    requests.post(url,data=json.dumps(data), headers=headers)

def UpdateField(elasticEndpoint,elsIndexName, elsIndexType, data, docId):
    # Write new field Property_Type to document
    url = '{0}/{1}/{2}/{3}/_update'.format(elasticEndpoint, elsIndexName, elsIndexType, docId)
    webresponse = requests.post(url,data=json.dumps(data))
    response = webresponse.json()
    return response
    
def CreateIndex(elsIndexNameName):
    '''Create an empty index'''
    elsIndexNameName = elsIndexNameName
    print('CREATE elsIndexName: {}/{}'.format(elasticEndpoint,elsIndexNameName))
    requests.put('{0}/{1}'.format(elasticEndpoint,elsIndexNameName))
    
    
def DeleteIndex(elsIndexNameName):
    '''Delete a full index from Elastic cluster'''
    elsIndexNameName = elsIndexNameName
    print('DELETE elsIndexName: {}/{}'.format(elasticEndpoint,elsIndexNameName))
    requests.delete('{0}/{1}'.format(elasticEndpoint,elsIndexNameName))


def TotalSearchRecords(elsIndexName, records):
    '''Search Elastic indices for specific JSON fields. Returns a JSON object.'''
    elsIndexName = elsIndexName      # elsIndexName name to search
    records = records    # Comma delimited list of records to search for, such as 'job__,block'

    url = '{}/{}/_search?_source={}'.format(elasticEndpoint, elsIndexName, records)
    webrequest = requests.get(url)
    results = webrequest.json()

    totalRecords = results['hits']['total']
    return totalRecords


def QueryIndex(indexName, query, scrollId=None):
    ''' Query Elastic index using scroll API. First call is used to obtain the scroll ID, then each
        subsequent call needs to pass the scroll ID to return next set of results'''
    while True:
        if scrollId == None:
            webrequest = requests.post('http://localhost:9200/{}/_search?scroll=1m'.format(indexName), data=json.dumps(query))
            results = webrequest.json()
            scrollId = results['_scroll_id']
            yield results
        else:
            webrequest = requests.post('http://localhost:9200/_search/scroll', data=json.dumps({"scroll": "1m", "scroll_id": '{}'.format(scrollId)}))
            results = webrequest.json()
            yield results
        # Break if no more data is available
        if len(results['hits']['hits']) == 0:
            break
    

def DeleteSnapshotRepository(repoName):
    url = '{}/_snapshot/{}'.format(elasticEndpoint,repoName)
    webrequest = requests.delete(url)
    results = webrequest.json()
    print(results)
    
    
def RemoveAllIndexReplicas():
    url = '{}/*/_settings'.format(elasticEndpoint)
    data = {
                "index" : {
                    "number_of_replicas" : 0
                }
            }
    webrequest = requests.put(url, data=json.dumps(data))
    results = webrequest.json()
    print(results)
    

def CreateSnapshot(repoName, indexPattern):
    ''' Create snapshot and remove replicas since this is a single node cluster '''
    now = datetime.datetime.now()
    snapshotName = '{}-{}-{}_{}-{}'.format(now.year, now.month, now.day, now.hour, now.minute)

    print('Creating snapshot:',snapshotName)
    RemoveAllIndexReplicas()
    url = '{}/_snapshot/{}/{}'.format(elasticEndpoint, repoName, snapshotName)
    data = {
              "indices": "indexPattern".format(indexPattern),
              "ignore_unavailable": "true",
              "include_global_state": "false"
            }
    webrequest = requests.put(url, data=json.dumps(data))
    results = webrequest.json()
    print(results)
    
def DeleteSnapshot(repoName, snapshotName):
    url = '{}/_snapshot/{}/{}'.format(elasticEndpoint, repoName, snapshotName)
    webrequest = requests.delete(url)
    results = webrequest.json()
    print(results)





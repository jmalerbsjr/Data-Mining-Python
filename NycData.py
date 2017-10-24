'''
Created on Jul 28, 2017

@author: JMalerbsJr
'''

import requests
import Elastic
import json

##### GLOBAL PARAMETERS #####
elasticEndpoint = Elastic.LocalEndpoint()
#############################
print('Setting Elastic end point to: {}'.format(elasticEndpoint))

class RawDataNyc:
    ''' Retrieve different types of raw data from NYC end points & write to Elastic '''

    def GetPublicJsonData(self, url):
        ''' GET JSON data from external URL '''

        self.url = url
        limit = 100
        offset = 1

        while True:
            # Return JSON Data
            webrequest = requests.get(url.format(limit, offset))
            results = webrequest.json()

            # Return data set and total number of records until no data is available
            if len(results) != 0:
                yield results
                offset = offset + limit
            else:
                break

    def Get(self, dataType):
        '''
            Retrieve specific data type from external end point and write to Elastic
            Required parameters:
                dataType: (ExternalSource1, ExternalSource3
        '''

        self.dataType = dataType.lower()
        docId = 1
        # Set parameters to get external data and write to Elastic indices
        if self.dataType == 'ExternalSource1':
            '''Get Application Data from NYC web site as JSON and write to ELS'''
            print('GET Application Filing Permit Data')
            url = 'http://data.cityofnewyork.us/resource-hx.json?$Limit={}&$Offset={}&$Order=bin__'
            elsIndexName = 'nyc-raw-index1'
            elsIndexType = 'permit'
        elif self.dataType == 'ExternalSource3':
            '''Get Issuance Data from NYC web site as JSON and write to ELS'''
            print('GET Permit Issuance Data')
            url = 'https://data.cityofnewyork.us/resource-x8.json?$Limit={}&$Offset={}&$Order=bin__'
            elsIndexName = 'nyc-raw-index2'
            elsIndexType = 'permit'
        elif self.dataType == 'ExternalSource2':
            '''Get Historical Data from NYC web site as JSON and write to ELS'''
            print('GET Historical Permit Issuance Data')
            url = 'https://data.cityofnewyork.us/resource-sz.json?$Limit={}&$Offset={}&$Order=bin'
            elsIndexName = 'nyc-raw-index2'
            elsIndexType = 'permit'
            docId = 1000000


        # Iterate through retrieved data set and write to Elastic
        for results in self.GetPublicJsonData(url):
            cleanDataObj = {}
            for dataObj in results:
                # Unify fields by stripping character off the end of field names
                for field in dataObj:
                    cleanDataObj[field.rstrip('_')] = dataObj[field]

                Elastic.WriteData(elasticEndpoint,elsIndexName,elsIndexType,cleanDataObj,docId)
                docId += 1




def GenerateUniqueBinNumbers(indexList: list):
    ''' Write a list of unique building ID numbers to Elastic from a list of given indices '''
    print('Creating list of unique Bin numbers from {}'.format(indexList))

    query = {"size": 10000,"_source" : ["bin"]}
    binList = []

    for indexName in indexList:
        print('Searching index: {}'.format(indexName))

        # Query Elastic for all Bin Numbers
        for results in Elastic.QueryIndex(indexName,query):
            # Append results to a list
            for result in results['hits']['hits']:
                try:
                    binList.append(result['_source']['bin'])
                except:
                    pass

    # Reduce Bin Number list to a unique SET
    uniqueBinList = set(binList)
    print('\t--Unique Records Found: {}'.format(len(uniqueBinList)))
    print('\t--Writing Records to Elastic')

    # Write list to Elastic
    for binNum in uniqueBinList:
        data = {"Bin_Num": '{}'.format(binNum)}
        docId = binNum
        Elastic.WriteData(elasticEndpoint, 'nyc-metadata-index1', 'metadata', data, docId)



def ReadBinListFromElastic():
    '''Pull Bin list from Elastic'''
    print('Query Elastic for Building Number List')

    indexName = 'nyc-metadata-index1'
    query = {"size": 1000,"_source" : ["Bin_Num"]}
    binNumList = []

    # Return a list of ALL Building Numbers
    for results in Elastic.QueryIndex(indexName,query):
        for result in results['hits']['hits']:
            binNumList.append(result['_source']['Bin_Num'])

    print('\t--Found: {}'.format(len(binNumList)))
    return binNumList



def GenerateBuildingInfo():
    ''' Generate building information:
            * Look for the first complete set of required fields from a list of indices.
            * If no record is found or all required fields aren't found in one of the results, then search next index.
    '''

    # Get list of unique building numbers from Elastic
    binNumList = ReadBinListFromElastic()
    binNumListCount = len(binNumList)

    print('Write building metadata to Elastic: {}'.format(binNumListCount))
    indexName = 'nyc-raw-index1'
    indexName2 = 'nyc-raw-index2'

    # Iterate through every unique Building Number
    for binNum in binNumList:
        docId = binNum
        query = {"size": 1,"query": {"match" : {"bin" : '{}'.format(binNum)}}}
        recordFound = False

        # Query first index for complete set of required indices
        for results in Elastic.QueryIndex(indexName, query):
            try:
                if results['hits']['hits']:
                    doc = results['hits']['hits'][0]['_source']

                    houseNum = doc['house']
                    streetName = doc['street_name']
                    zipCode = doc['zip']
                    borough = doc['borough']
                    blockNum = doc['block']
                    lotNum = doc['lot']

                    recordFound = True
                    break
            except:
                pass

        # If first index doesn't return data for all required fields in one of the results returned, then query second index for complete set of required indices
        if recordFound == False:
            for results in Elastic.QueryIndex(indexName2, query):
                try:
                    if results['hits']['hits']:
                        doc = results['hits']['hits'][0]['_source']

                        houseNum = doc['house']
                        streetName = doc['street_name']
                        zipCode = doc['zip_code']
                        borough = doc['borough']
                        blockNum = doc['block']
                        lotNum = doc['lot']

                        recordFound = True
                        break
                except:
                    pass

        # Write standardized documents to Elastic if all required fields have data
        if recordFound == True:
            docId = binNum
            data = {'Bin' : binNum.rstrip(),
            'House_Num' : houseNum.rstrip(),
            'State' : 'NY',
            'Street_Name' : streetName.rstrip().title(),
            'Zip_Code' : zipCode.rstrip(),
            'Borough' : borough.rstrip().title(),
            'Block_Num' : blockNum.rstrip(),
            'Lot_Num' : lotNum.rstrip() }
            Elastic.WriteData(elasticEndpoint, 'nyc-metadata-index2', 'permit', data, docId)
        else:
            # Record was not found in either index or Elastic documents didn't contain one of the required fields
            print('No record or missing data for', binNum)




def GenerateUniqueGeneralContractorNumbers(indexList: list):
    ''' Write a list of unique General Contractor Permit License Numbers from a list of given indices to Elastic'''

    print('Creating list of unique General Contractor License numbers from {}'.format(indexList))

    query = {"size": 1000,"_source" : ["permittee_s_license"]}
    gcList = []

    for indexName in indexList:
        print('Searching index: {}'.format(indexName))

        for results in Elastic.QueryIndex(indexName,query):

            for result in results['hits']['hits']:
                try:
                    gcList.append(result['_source']['permittee_s_license'])
                except:
                    pass

    uniqueGcList = set(gcList)
    print('\t--Unique Records Found: {}'.format(len(uniqueGcList)))
    print('\t--Writing Records to Elastic')
    for gc in uniqueGcList:
        data = {"Permitees_Lic_Num": '{}'.format(gc)}
        docId = gc
        Elastic.WriteData(elasticEndpoint, 'nyc-metadata-index3', 'metadata', data, docId)



def ReadGeneralContractoristFromElastic():
    '''Pull Bin list from Elastic'''
    print('Query Elastic for General Contractor License Number List')

    indexName = 'nyc-metadata-index3'
    query = {"size": 1000,"_source" : ["Permitees_Lic_Num"]}
    gcNumList = []

    # Get list of ALL Building Numbers
    for results in Elastic.QueryIndex(indexName,query):

        for result in results['hits']['hits']:
            gcNumList.append(result['_source']['Permitees_Lic_Num'])

    return gcNumList



def GenerateGeneralContractorInfo():
    '''Use Bin list to get Bin Data and write to Elastic'''

    # Get list of building numbers from Elastic
    gcNumList = ReadGeneralContractoristFromElastic()

    print('Write building metadata to Elastic: {}'.format(len(gcNumList)))
    indexName = 'nyc-raw-index2'

    for gcNum in gcNumList:
        # Check sequence of indices for data where it exists
        query = {"size": 1,"query": {"match" : {"permittee_s_license" : '{}'.format(gcNum)}}}

        for results in Elastic.QueryIndex(indexName, query):
            try:
                doc = results['hits']['hits'][0]
                Gc_Lic_Num = doc['_source']['permittee_s_license']
                Gc_Business_Name = doc['_source']['permittee_s_business_name']
                Gc_Lic_Type = doc['_source']['permittee_s_license_type']

                docId = gcNum
                data = {'Gc_Lic_Num' : Gc_Lic_Num.rstrip(),
                        'Gc_Business_Name' : Gc_Business_Name.rstrip().title(),
                        'Gc_Lic_Type' : Gc_Lic_Type.rstrip().title()
                }

                Elastic.WriteData(elasticEndpoint, 'nyc-metadata-Index3', 'permit', data, docId)
                break
            except:
                break




def AssignBuildingOccupancy():
    ''' Update Building Information documents to include a Property_Type field, indicating the type of dwelling. Ex: Hotel, Appartment, etc... '''



    # Get list of building numbers from Elastic to iterate through

    binNumList = ReadBinListFromElastic()
    binNumListCount = len(binNumList)

    # Retrieve documents based on dictionary defined below
    elsIndexName = 'nyc-raw-index1'
    elsIndexType = 'permit'
    updateElsIndexName = 'nyc-metadata-index2'
    propertyTypeDic = {
                       "Hotel":["J-1","R-1"],
                       "Residential":["J-2","R-2"],
                       "Commercial":["B","COM","E"]
                    }

    print('Updating index {} with Property_Type attribute'.format(updateElsIndexName))
    count = 0
    for binNum in binNumList:
        count += 1
        # Retrieve each Building Number & process records
        query = {"size": 100,"sort" : [{ "dobrundate" : {"order" : "desc"}}],"query": {"match" : {"bin" : '{}'.format(binNum)}}}
        for results in Elastic.QueryIndex(elsIndexName ,query):
            for result in results['hits']['hits']:

                # Preset Property Type, if no records match the propertyTypeDic
                data = {"doc" : {"Property_Type" : "Other" }}
                try:

                    # Use most current record that has an approved permit status
                    if "P" == result['_source']['job_status'] or "R" == result['_source']['job_status']:
                        docId = binNum

                        # Set property type
                        for propertyType in propertyTypeDic:
                            for occupancyType in propertyTypeDic[propertyType]:
                                if result['_source']['proposed_occupancy'] in occupancyType:
                                    data = {"doc" : {"Property_Type" : propertyType }}

                        # Write field data to Elastic & remove Building Number from process space
                        Elastic.UpdateField(elasticEndpoint, updateElsIndexName, elsIndexType, data, docId)
                        binNumList.remove(binNum)
                        print(elasticEndpoint, updateElsIndexName, elsIndexType, data, docId)
                        break
                except:
                    pass
            break

        # Print progress
        # ****FIX THIS****
        if count%.10 == 0:
            print('{} of {}'.format(count, binNumListCount))





'''------------------- CONTROL CENTER -----------------------'''
''' START OVER - DELETE ALL DATA '''
#Elastic.DeleteIndex("nyc-raw-index1")
#Elastic.DeleteIndex("nyc-raw-index2")
#Elastic.DeleteIndex("nyc-metadata-index1")
#Elastic.DeleteIndex("nyc-metadata-index2")
#Elastic.DeleteIndex("nyc-metadata-index3")

'''Pull data from external sources'''
#RawDataNyc().Get('ExternalSource1')                            # Pull NYC App Filing Permit data into Elastic
#RawDataNyc().Get('ExternalSource2')                   # Pull NYC Historical Issuance Permit data into Elastic
#RawDataNyc().Get('ExternalSource3')                             # Pull NYC Issuance Permit data into Elastic


'''Data Reduction & metadata generation'''
#GenerateUniqueBinNumbers(['nyc-raw-index1','nyc-raw-index2'])       # Write unique Building Numbers to Elastic
#GenerateBuildingInfo()                                                  # Write building location information & standardize field names
#GenerateUniqueGeneralContractorNumbers(['nyc-raw-index2'])            # Write unique General Contractor license numbers to Elastic
#GenerateGeneralContractorInfo()                                        # Write General Contractor business information & standardize field names
#AssignBuildingOccupancy()                                               # Assign property type to Building Information (Hotel, Appartment, etc...)


''' BACKUP DATA '''
#Elastic.CreateSnapshot("local_snapshots","nyc*")
#DeleteSnapshot(repoName, snapshotName)
#Elastic.RemoveAllIndexReplicas()
'''-----------------------------------------------------------'''

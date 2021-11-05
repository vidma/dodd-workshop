def get_cookie(damurl,PAT):
    import requests
    session = requests.Session()
    cookieurl=damurl + '/api/auth/callback?client_name=ExternalAppTokenClient'
    header = {'X-External-App-Token':PAT}
    response = session.post(url=cookieurl,headers=header,verify=False)
    cookie = session.cookies
    return cookie

def get_datasources(url,cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/datasources",cookies=cookie,verify=False)
    return v.json()

def get_datasources_and_lineages(url,cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/datasources?includeLineageCounts=true",cookies=cookie,verify=False)
    return v.json()


def get_lineage_for_schema(url, cookie, schemaID):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/lineages/with-input-schema/%s" %schemaID,cookies=cookie,verify=False)
    return v.json() 

def get_lineage_for_ds(url, cookie, dsID):
    import requests
    v = requests.get(url+"/services/v1/lineage/%s/export-usages-lineage?granularity=per-table&logical=false" %dsID,cookies=cookie,verify=False)
    return v.json() 

def get_processes_for_project(url, cookie, projectID):
    import requests
    v = requests.get(url+"/business/api/views/v1/project-catalog/process-runs?projectId=%s" %projectID,cookies=cookie,verify=False)
    try:
        return [item['process']['id'] for item in v.json()['data']['nodes']]
    except:
        return []


def get_datasources_names(url,cookie):
    import requests
    v = get_datasources(url,cookie)
    r=[]
    for i in v:
        r.append(i['name'])
    return r

def get_processes_names(url,cookie):
    import requests
    v = get_processes_only(url,cookie)
    r=[]
    for i in v:
        r.append(i['name'])
    return r

def get_processes(url,cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/data-processes?includeRunsInfo=true",cookies=cookie,verify=False)
    return v.json() 

def get_processes_only(url,cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/data-processes?includeRunsInfo=false",cookies=cookie)
    return v.json() 

def get_process(url, cookie, processId):
    import requests
    v = requests.get(url+"/api/services/v1/resources/data-process/%s" %processId,cookies=cookie)
    return v.json() 

def get_process_runs(url, cookie):
    import requests
    v = requests.get(url+"/dev-unofficial/resources/process-runs", cookies = cookie)
    return v.json()

def get_categories(url, cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/datasource-categories",cookies=cookie)
    return v.json() 

def get_datasource(url, cookie, dsId):
    import requests
    v = requests.get(url+"/api/services/v1/resources/datasource/%s" %dsId,cookies=cookie)
    return v.json() 

def get_schema(url, cookie, schemaId):
    import requests
    v = requests.get(url+"/api/services/v1/resources/schema/%s" %schemaId,cookies=cookie)
    return v.json() 

def get_lineage(url, cookie, lineageId):
    import requests
    v = requests.get(url+"/api/services/v1/resources/datasource-lineage/%s" %lineageId,cookies=cookie,verify=False)
    return v.json() 

def get_output_datasources(url, cookie, dsId):
    import requests
    v = requests.get(url+"/api/services/v1/datasource-lineage/usages-of-datasource/%s" %dsId,cookies=cookie)
    return v.json() 

def get_input_datasources(url, cookie, dsId):
    import requests
    v = requests.get(url+"/api/services/v1/datasource-lineage/dependencies-by-output-datasource/%s" %dsId,cookies=cookie)
    return v.json() 

def get_logical_categories(url, cookie):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/logical-datasource-names",cookies=cookie)
    return v.json() 

def get_datasources_in_category(url, cookie, logical):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/datasources/in-logical/%s" %logical,cookies=cookie)
    return v.json() 

def get_schemas_in_datasource(url, cookie, datasource):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/schemas/describing/%s" %datasource,cookies=cookie)
    return v.json() 

def get_input_processlineages(url, cookie, schema):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/lineages/with-input-schema/%s" %schema,cookies=cookie)
    return v.json() 

def get_output_processlineages(url, cookie, schema):
    import requests
    v = requests.get(url+"/api/services/v1/experimental/lineages/with-output-schema/%s" %schema,cookies=cookie)
    return v.json() 

def get_projects(url, cookie):
    import requests
    v = requests.get(url+"/api/services/v1/resources/projects",cookies=cookie,verify=False)
    return v.json() 


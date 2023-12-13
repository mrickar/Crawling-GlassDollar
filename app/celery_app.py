from celery import Celery, group
from celery.result import allow_join_result
import requests
from redis_utils import  redis_clear_all
import time
from celery.result import AsyncResult
import os


# BROKER_URL = "redis://redis/0"
# BACKEND_URL = "redis://redis/1"
# celery_app = Celery("tasks",broker= BROKER_URL,backend=BACKEND_URL)
celery_app = Celery("celery_app")

celery_app.conf.broker_url=os.environ.get('CELERY_BROKER_URL',"redis://redis/0")
celery_app.conf.result_backend=os.environ.get('CELERY_RESULT_BACKEND',"redis://redis/1")

@celery_app.task
def get_enterprise_details(id):

    json_data = {
        'variables': {
            'id': id,
        },
        'query': 'query ($id: String!) {\n  corporate(id: $id) {\n       name\n    description\n    logo_url\n    hq_city\n    hq_country\n    website_url\n    linkedin_url\n    twitter_url\n    startup_partners_count\n    startup_partners {\n          company_name\n      logo_url: logo\n      city\n      website\n      country\n      theme_gd\n         }\n    startup_themes\n    }\n}\n',
    }

    response = requests.post('https://ranking.glassdollar.com/graphql', json=json_data)
    if response.status_code != 200:
        raise ValueError(f"Error: response.status_code while getting details of enterprise with id: {id} ({response.status_code})")
    
    #List of id's of enterprises
    enterprise_details = response.json()["data"]["corporate"]
    return enterprise_details
    
@celery_app.task
def get_enterprise_ids_by_page(page_number):
    #GraphQL query json
    json_data = {
        'variables': {
            "filters": {
                "hq_city": [],
                "industry":[]
            }
        },
        'query': 'query($filters:CorporateFilters) { corporates(filters:$filters,page:'+str(page_number)+'){  rows{name id}  count}}',
    }
    request_url = "https://ranking.glassdollar.com/graphql"
    response = requests.post(request_url, json=json_data)
    
    if response.status_code != 200:
        raise ValueError(f"Error: response.status_code while getting enterprise ids: {response.status_code}")
    #List of id's of enterprises
    enterprise_id_list = [ id_dict["id"] for id_dict in response.json()["data"]["corporates"]["rows"]] 
    return enterprise_id_list

@celery_app.task
def get_corporate_count():

    #GraphQL query json
    json_data = {
        'variables': {},
        'query': 'query{getCorporateCount}',
    }
    request_url = "https://ranking.glassdollar.com/graphql"
    response = requests.post(request_url,json=json_data)
    if response.status_code != 200:
        raise ValueError(f"Error: response.status_code while getting enterprise ids: {response.status_code}")
    #Total count of enterprises
    return response.json()["data"]["getCorporateCount"] 

def get_all_enterprise_ids(PAGE_SIZE):
    #Getting total corparate count
    total_enterprise_count = get_corporate_count()
    print(f"Total Enterprise Count:{total_enterprise_count}")
    
    #How many pages needed to sent as request to GraphQL
    total_page_count = total_enterprise_count// PAGE_SIZE + 1
    
    #Creating a group of parallel jobs for getting id's of all enterprises
    getting_all_ids_job = group([get_enterprise_ids_by_page.s(page_number) for page_number in range(1,total_page_count+1)])
    
    #Running the jobs
    enterprise_ids_result = getting_all_ids_job.apply_async()
    
    #Waiting for all jobs to finish and collect the result
    print("######################")
    with allow_join_result():
        enterprise_ids = enterprise_ids_result.get() #Returned values from all get_enterprise_ids_by_page function as list of list
    print("---------------------")
    if enterprise_ids_result.successful():
        print("Getting id's of enterprises has finished successfully")
    else:
        raise ValueError(f"Some error has occured during getting id's.")
    enterprise_ids = [id for sublist in enterprise_ids for id in sublist]
    print(f"Total {len(enterprise_ids)} id's has crawled.")
    return enterprise_ids

def get_all_enterprise_details(enterprise_ids:list):
    
    #Creating a group of parallel jobs for getting details of enterprises
    getting_ent_details_job = group([get_enterprise_details.s(id) for id in enterprise_ids])
    
    #Running the jobs
    enterprise_details_result = getting_ent_details_job.apply_async()
    
    #Waiting for all jobs to finish and collect the result
    with allow_join_result():
        enterprise_details_list = enterprise_details_result.get() #Returned values from all get_enterprise_ids_by_page function as list of dictionaries
    
    if enterprise_details_result.successful():
        print("Getting details of enterprises has finished successfully")
    else:
        raise ValueError(f"Some error has occured during getting details.")
    
    print(f"Total {len(enterprise_details_list)} enterprise details has crawled.")
    return enterprise_details_list

@celery_app.task
def celery_main():
    start_time= time.time()
    
    #Constant page size in GraphQL requests of the website
    PAGE_SIZE = 32
    
    #Creating a list containing id's of all enterprise
    enterprise_ids = get_all_enterprise_ids(PAGE_SIZE)
    
    # Creating a list containing wanted details of all enterprise
    enterprise_details_list = get_all_enterprise_details(enterprise_ids) 
    
    #Clear redis backend db
    # redis_clear_all() 
    
    end_time=time.time()
    print(f"Time: {end_time - start_time}")
    
    return enterprise_details_list

def get_task_result(task_id):
    return AsyncResult(task_id,app=celery_app)


#This was another implementation version of the celery_main func
# @celery_app.task
# def celery_main2():
#     start_time= time.time()
#     #Constant page size in GraphQL requests of the website
#     PAGE_SIZE = 32
    
#     #Getting total corparate count
#     total_enterprise_count = get_corporate_count()
#     print(f"Total Enterprise Count:{total_enterprise_count}")
    
#     #How many pages needed to sent as request to GraphQL
#     total_page_count = total_enterprise_count// PAGE_SIZE + 1
    
#     #List which will store AsyncResult of jobs
#     enterprise_details_result_lst=[]
    
#     for page_number in range(1,total_page_count+1):
#         #Starting get_enterprise_ids_by_page task
#         cur_id_lst_result = get_enterprise_ids_by_page.delay(page_number)
        
#         #Getting the value
#         cur_id_lst = cur_id_lst_result.get()
        
#         #If the result is unsuccessful raise and error
#         if cur_id_lst_result.successful()==False:
#             raise ValueError(f"Some error has occured during getting id's of page {page_number}.")
        
#         #Creating group of jobs for getting details of that page
#         cur_ent_details_job = group([get_enterprise_details.s(id) for id in cur_id_lst])
        
#         #Running jobs and appending AsyncResult to list
#         enterprise_details_result_lst.append(cur_ent_details_job.apply_async())
    
#     #Crawled enterprise details data
#     enterprise_details_list = []
    
#     #Collecting details of enterprises on each page
#     for page_number in range(1,total_page_count+1):
#         #Waiting enterprise details for current page
#         enterprise_details_list.extend(enterprise_details_result_lst[page_number-1].get())
        
#         if enterprise_details_result_lst[page_number-1].successful() == False:
#             raise ValueError(f"Some error has occured during getting details of page {page_number}.")
            
            
#     #Clear redis backend db
#     redis_clear_all()
#     print(f"Total {len(enterprise_details_list)} enterprise details has crawled.")
#     end_time=time.time()
#     print(f"Time: {end_time - start_time}")
#     return enterprise_details_list

    return AsyncResult(task_id,app=celery_app)
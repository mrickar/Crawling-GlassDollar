from celery import Celery, group
from celery.result import allow_join_result
import requests
from redis_utils import  redis_clear_all
import time
from celery.result import AsyncResult
import os


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
    
    enterprise_details = response.json()["data"]["corporate"]
    return enterprise_details
    
@celery_app.task
def get_enterprise_ids_by_page(page_number):
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
    enterprise_id_list = [ id_dict["id"] for id_dict in response.json()["data"]["corporates"]["rows"]] 
    return enterprise_id_list

@celery_app.task
def get_corporate_count():

    json_data = {
        'variables': {},
        'query': 'query{getCorporateCount}',
    }
    request_url = "https://ranking.glassdollar.com/graphql"
    response = requests.post(request_url,json=json_data)
    if response.status_code != 200:
        raise ValueError(f"Error: response.status_code while getting enterprise ids: {response.status_code}")
    return response.json()["data"]["getCorporateCount"] 

def get_all_enterprise_ids(PAGE_SIZE):
    total_enterprise_count = get_corporate_count()
    print(f"Total Enterprise Count:{total_enterprise_count}")
    
    total_page_count = total_enterprise_count// PAGE_SIZE + 1
    
    getting_all_ids_job = group([get_enterprise_ids_by_page.s(page_number) for page_number in range(1,total_page_count+1)])
    
    enterprise_ids_result = getting_all_ids_job.apply_async()
    

    with allow_join_result():
        enterprise_ids = enterprise_ids_result.get()
        
    if enterprise_ids_result.successful():
        print("Getting id's of enterprises has finished successfully")
    else:
        raise ValueError(f"Some error has occured during getting id's.")
    enterprise_ids = [id for sublist in enterprise_ids for id in sublist]
    print(f"Total {len(enterprise_ids)} id's has crawled.")
    return enterprise_ids

def get_all_enterprise_details(enterprise_ids:list):
    
    
    getting_ent_details_job = group([get_enterprise_details.s(id) for id in enterprise_ids])
    
    
    enterprise_details_result = getting_ent_details_job.apply_async()
    
    
    with allow_join_result():
        enterprise_details_list = enterprise_details_result.get()
    
    if enterprise_details_result.successful():
        print("Getting details of enterprises has finished successfully")
    else:
        raise ValueError(f"Some error has occured during getting details.")
    
    print(f"Total {len(enterprise_details_list)} enterprise details has crawled.")
    return enterprise_details_list

@celery_app.task
def celery_main(clear_after_proc=False):
    start_time= time.time()
    
    PAGE_SIZE = 32
    
    enterprise_ids = get_all_enterprise_ids(PAGE_SIZE)
    
    enterprise_details_list = get_all_enterprise_details(enterprise_ids) 
    
    if clear_after_proc:
        redis_clear_all() 
    
    end_time=time.time()
    print(f"Time: {end_time - start_time}")
    
    return enterprise_details_list

def get_task_result(task_id):
    return AsyncResult(task_id,app=celery_app)

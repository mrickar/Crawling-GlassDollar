from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from celery_app import celery_main,get_task_result

fastapi_app = FastAPI()

#Endpoint for crawling enterprises
@fastapi_app.get("/enterprises/")
def get_enterprise_data():
    #Initiates the crawling process on celery
    enterprise_data_result = celery_main.delay()
    #Redirects user for the endpoint spesific to the task id
    redirection_url = f"/requests/{enterprise_data_result}"
    return {"Status":"Success","Message":"Your request has started to process check your results in provided end point.","EndPoint":redirection_url}
    
    
#Checking the task results
@fastapi_app.get("/requests/{task_id}")
def get_enterprise_data(task_id:str):
    
    task_result = get_task_result(task_id)
    #If ready send it
    if task_result.ready():
        enterprise_data = task_result.get()
        return  {"result": enterprise_data}
    #If not ready display come back later message
    else:
        return {"Status": "Task in progress. Please check the new endpoint in a short time for the result."}

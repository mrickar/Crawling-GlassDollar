from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from celery_app import celery_main,get_task_result

fastapi_app = FastAPI()

@fastapi_app.get("/enterprises/")
def get_enterprise_data():
    enterprise_data_result = celery_main.delay()
    redirection_url = f"/requests/{enterprise_data_result}"
    return {"Status":"Success","Message":"Your request has started to process check your results in provided end point.","EndPoint":redirection_url}
    
    
@fastapi_app.get("/requests/{task_id}")
def get_enterprise_data(task_id:str):
    
    task_result = get_task_result(task_id)
    if task_result.ready():
        enterprise_data = task_result.get()
        return  {"result": enterprise_data}
    else:
        return {"Status": "Task in progress. Please check the new endpoint in a short time for the result."}

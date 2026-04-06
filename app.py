
from fastapi import FastAPI, Request, Body
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

#https://www.mongodb.com/resources/languages/pymongo-tutorial
ATLAS_URI="mongodb+srv://beiyu:397975944@hw3.yjd1snr.mongodb.net/?appName=hw3"
DB_NAME="ev_db"
COLLECTION_NAME="vehicles"
CLIENT=MongoClient(ATLAS_URI)
COLLECTION=CLIENT[DB_NAME][COLLECTION_NAME]

app = FastAPI()

@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(ATLAS_URI)
    app.database = app.mongodb_client["ev_db"]
    print("Connected")

@app.post("/insert-fast")
def insert_fast(request:Request,ev:dict=Body(...)):
    collection=request.app.database["vehicles"].with_options(write_concern=WriteConcern(w=1))
    new_vehicle=collection.insert_one(ev)
    return str(new_vehicle.inserted_id)

@app.post("/insert-safe")
def insert_safe(request:Request,ev:dict=Body(...)):
    collection=request.app.database["vehicles"].with_options(write_concern=WriteConcern(w="majority"))
    new_vehicle=collection.insert_one(ev)
    return str(new_vehicle.inserted_id) 

@app.get("/count-tesla-primary")
def count_tesla_primary(request:Request):
    collection=request.app.database["vehicles"].with_options(read_preference=ReadPreference.PRIMARY)
    count=collection.count_documents({"make": "TESLA"})
    return {"count":count}


@app.get("/count-bmw-secondary")
def count_bmw_secondary(request:Request):
    collection=request.app.database["vehicles"].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count=collection.count_documents({"make": "BMW"})
    return {"count":count}
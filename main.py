from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "root ok"}

@app.get("/ping")
def ping():
    return {"message": "ping ok"}

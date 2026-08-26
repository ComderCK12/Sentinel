from fastapi import FastAPI

app = FastAPI(title="Sentinel Model Serving")

SERVICE_NAME = "model-serving"

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME}
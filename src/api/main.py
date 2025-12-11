from fastapi import FastAPI

app = FastAPI(title="FinCompass Orchestrator")

@app.get("/health")
def health():
    return {"status": "ok"}

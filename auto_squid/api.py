from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="auto_squid - API")

class Health(BaseModel):
    status: str


@app.get("/health", response_model=Health)
async def health():
    return {"status": "ok"}


@app.get("/score")
async def get_scores(domain: str | None = None):
    """Stub: return scores for domain/proxies. Implement scoring lookup."""
    # TODO: wire to domain_index / proxy_store
    return {"domain": domain, "scores": []}

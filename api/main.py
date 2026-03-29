from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import query, schema, optimizer, audit

app = FastAPI(
    title="SQLens API",
    description="HTTP interface to the SQLens MCP tools.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router)
app.include_router(schema.router)
app.include_router(optimizer.router)
app.include_router(audit.router)


@app.get("/")
def root():
    return {
        "name":    "sqlens",
        "version": "0.1.0",
        "docs":    "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok"}

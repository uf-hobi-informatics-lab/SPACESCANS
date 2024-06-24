from fastapi import FastAPI, Request, HTTPException
import time
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Display the available exposome catalog
@app.post("/catalog")
async def show_catalog():
    print("")


# Frontend user uploaded data
@app.post("/upload")
async def upload():
    print("")


if __name__=="__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
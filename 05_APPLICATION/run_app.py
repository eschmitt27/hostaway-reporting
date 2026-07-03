import uvicorn
from app.config import PORT, LOG_LEVEL

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=PORT,
        reload=False,
        log_level=LOG_LEVEL,
    )

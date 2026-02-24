from __future__ import annotations

import uvicorn


if __name__ == "__main__":
    uvicorn.run("app.web_api:app", host="0.0.0.0", port=8089, reload=False)

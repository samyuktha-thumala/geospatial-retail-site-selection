import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from .router import api
from .data.store import db

app = FastAPI(title="Site Selection Platform", version="0.1.0")
app.include_router(api)


@app.get("/health")
async def health():
    """Debug health endpoint — shows data mode and env."""
    return {
        "data_mode": db.mode,
        "locations_count": len(db.locations),
        "is_databricks_app": bool(os.environ.get("DATABRICKS_APP_NAME")),
        "warehouse_id": os.environ.get("DATABRICKS_WAREHOUSE_ID", "NOT SET"),
        "has_databricks_host": bool(os.environ.get("DATABRICKS_HOST")),
        "_debug_main_file": str(Path(__file__)),
        "_debug_ui_dist": str(_ui_dist),
        "_debug_ui_dist_exists": _ui_dist.exists(),
        "_debug_slides_exists": (_ui_dist / "slides").exists(),
        "_debug_dist_contents": [p.name for p in _ui_dist.iterdir()] if _ui_dist.exists() else [],
    }

# Serve React static build if it exists
_ui_dist = Path(__file__).parent.parent / "ui" / "dist"
if _ui_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_ui_dist / "assets")), name="assets")
    if (_ui_dist / "slides" / "assets").exists():
        app.mount("/slides/assets", StaticFiles(directory=str(_ui_dist / "slides" / "assets")), name="slides-assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = _ui_dist / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        # If it's a directory with its own index.html, serve that
        if file_path.is_dir() and (file_path / "index.html").exists():
            return FileResponse(str(file_path / "index.html"))
        return FileResponse(
            str(_ui_dist / "index.html"),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

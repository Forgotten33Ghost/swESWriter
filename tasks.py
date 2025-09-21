# tasks_way.py
from typing import List, Dict, Any
from celery_app import app
from celery.utils.log import get_task_logger
from es_writer import index_docs_to_way

log = get_task_logger(__name__)

@app.task(name="esWriter", queue="to_es",
          autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, retry_kwargs={"max_retries": 5})
def ingest_way_serial(docs: List[Dict[str, Any]], *, chunk_size: int = 2000, refresh: str = "false") -> Dict[str, Any]:
    res = index_docs_to_way(docs, chunk_size=chunk_size, refresh=refresh)
    if res["errors_count"]:
        log.warning("ES bulk had %s errors, sample=%s", res["errors_count"], res["errors_sample"])
    return res
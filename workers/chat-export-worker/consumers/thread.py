"""v1 full flow: resolve -> normalize -> package -> upload -> sign -> complete.

Uses Job.type (not job_type), Job.spec (not input_params),
Job.result (not output_params). JobType.EXPORT + spec["kind"]="chat_export_thread"
is the discriminator.
"""
import json
import logging
import os
import tempfile
import aio_pika
from sqlalchemy import select, update

from shared.database import async_session_factory
from shared.models import Job, JobStatus, Resource
from shared.azure_storage import (
    azure_storage_manager,
    upload_blob_with_retry_from_file,
)
from shared.config import settings

from workers.chat_export_worker.progress import publish
from workers.chat_export_worker.scope import resolve
from workers.chat_export_worker.render.normalizer import normalize_messages
from workers.chat_export_worker.packager.thread_packager import (
    ThreadPackager, AttachmentSource, _safe_name,
)

log = logging.getLogger("chat-export.thread")


class BlobAttachmentSource(AttachmentSource):
    """Stream bytes from the shared tenant blob store.

    True streaming via `download_blob_stream` — yields ~4 MiB chunks
    from the Azure SDK so the worker's RAM footprint stays bounded
    regardless of the attachment size. Pre-fix this method buffered
    the entire blob into memory before returning, which on a 310 MB
    chat attachment would spike RAM by exactly that much. With the
    streaming path the peak is one chunk (~4 MiB) plus the ZIP
    entry's compression buffer (~16 KiB).
    """

    async def open(self, blob_path: str):
        from shared.azure_storage import azure_storage_manager

        # Prefer the project's open_stream helper if it ever lands.
        # Today it doesn't exist, so we fall through to the
        # download_blob_stream path on the default shard.
        if hasattr(azure_storage_manager, "open_stream"):
            return await azure_storage_manager.open_stream(blob_path)

        shard = azure_storage_manager.get_default_shard()
        # blob_path in DB is of the form "<container>/<path>" or just "<path>".
        # backup-worker writes chat blobs under "tenant-data" by default;
        # exports / chat-blobs are alternate prefixes seen in old data.
        container = "tenant-data"
        path = blob_path
        if "/" in blob_path:
            head, tail = blob_path.split("/", 1)
            if head in {"tenant-data", "exports", "chat-blobs"}:
                container, path = head, tail

        # Probe size up-front (informational only — the consumer
        # doesn't currently use it but downstream callers might).
        size = 0
        try:
            props = await shard.get_blob_properties(container, path)
            if props:
                size = int(props.get("size") or 0)
        except Exception:
            pass

        async def _stream():
            async for chunk in shard.download_blob_stream(container, path):
                if chunk:
                    yield chunk

        return _stream(), "application/octet-stream", size


async def _update_job(sess, job_id, **values):
    await sess.execute(update(Job).where(Job.id == job_id).values(**values))
    await sess.commit()


async def _check_cancelled(sess, job_id) -> bool:
    from sqlalchemy import select
    from shared.models import Job, JobStatus
    st = (await sess.execute(select(Job.status).where(Job.id == job_id))).scalar_one()
    return st == JobStatus.CANCELLING


async def consume_thread(message: aio_pika.IncomingMessage) -> None:
    async with message.process(requeue=False):
        body = json.loads(message.body)
        job_id = body["jobId"]
        log.info("job_started id=%s", job_id)
        await publish(job_id, "progress", {"stage": "resolving", "percent": 5})

        async with async_session_factory() as sess:
            job = (await sess.execute(select(Job).where(Job.id == job_id))).scalar_one()
            if job.status == JobStatus.CANCELLING:
                await _update_job(sess, job_id, status=JobStatus.CANCELLED)
                await publish(job_id, "cancelled", {})
                return
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                # Terminal already — e.g. a broker redelivery after a crash
                # that landed between the COMPLETED commit and the ack, or a
                # job an operator/reclaim already failed. Re-running would
                # resurrect it (FAILED -> COMPLETED flip-flop) and duplicate
                # the export.
                log.info("job_skipped_terminal id=%s status=%s", job_id, job.status)
                return

            spec = job.spec or {}
            try:
                scope = await resolve(
                    sess,
                    resource_id=spec["resourceId"],
                    snapshot_ids=spec["snapshotIds"],
                    thread_path=spec.get("threadPath"),
                    item_ids=spec.get("itemIds") or [],
                )
            except ValueError as e:
                await _update_job(
                    sess, job_id, status=JobStatus.FAILED,
                    result={"error": {"code": str(e)}},
                )
                await publish(job_id, "error", {"code": str(e)})
                return

            # The rest of the pipeline (normalize → render → package → upload
            # → sign) is wrapped in a try/except so an unexpected exception
            # transitions the Job row to FAILED instead of leaving it stuck
            # in RUNNING. RabbitMQ acks the message on exit (requeue=False
            # on message.process) but does not touch the DB, so without
            # this block the UI would poll forever after any failure.
            try:
                await _run_pipeline(sess, job_id, spec, scope, job)
            except Exception as exc:
                log.exception("job_failed id=%s", job_id)
                # Discard whatever the pipeline left uncommitted so the
                # session is usable again (a DB error mid-pipeline poisons
                # the transaction), then write FAILED — but never over a
                # terminal state: the pipeline may already have committed
                # COMPLETED (a late completion-publish blip lands here) and
                # clobbering it erases result.blob_path, stranding the
                # uploaded ZIP with no DB row pointing at it.
                await sess.rollback()
                await sess.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .where(Job.status.notin_([JobStatus.COMPLETED, JobStatus.CANCELLED]))
                    .values(
                        status=JobStatus.FAILED,
                        result={"error": {"code": "pipeline_error", "message": str(exc)}},
                    )
                )
                await sess.commit()
                await publish(job_id, "error", {"code": "pipeline_error", "message": str(exc)})


async def _run_pipeline(sess, job_id, spec, scope, job) -> None:
    await publish(
        job_id, "progress",
        {"stage": "rendering", "percent": 20,
         "messagesTotal": len(scope.messages)},
    )

    # Inline image src rewriter: the path stored here is the path the
    # rendered HTML's <img src=...> will use. It MUST match the path
    # the packager writes the file at, OTHERWISE every inline image
    # in the export is a broken image link.
    #
    # Two layout-dependent shapes (mirrors packager._add paths exactly):
    #   single_thread (HTML at zip root, /{tsafe}.html):
    #     <img src="./{tsafe}-attachments/inline/{hc_id}{ext}">
    #   per_message (HTML at /per-message/{tsafe}/{ext_id}/{ext_id}.html):
    #     <img src="./attachments/inline/{hc_id}{ext}">
    #
    # tsafe MUST be _safe_name(thread_name) — the same sanitiser the
    # packager applies. Pre-fix code used `thread_path.split('/')[-1]`
    # raw, which left colons / slashes / spaces unsanitised in the
    # src URL while the packager wrote to the sanitised path on disk.
    # Teams chats like "Group: A, B, C" hit this every time.
    thread_name = scope.thread_path.rsplit("/", 1)[-1]
    tsafe = _safe_name(thread_name)
    hosted_by_msg: dict = {}
    for mid, hs in scope.hosted_map.items():
        msg_tsafe = _safe_name(mid)
        for h in hs:
            if scope.layout == "single_thread":
                local_path = f"./{tsafe}-attachments/inline/{h['hc_id']}{h.get('ext', '.bin')}"
            else:
                # per_message: HTML lives in
                # per-message/{tsafe}/{ext_id}/, attachments dir
                # is one level deeper. Relative-from-HTML.
                local_path = f"./attachments/inline/{h['hc_id']}{h.get('ext', '.bin')}"
            hosted_by_msg.setdefault(mid, []).append({
                "hc_id": h["hc_id"],
                "local_path": local_path,
            })

    render_messages = normalize_messages(
        scope.messages,
        attachments_by_msg={},
        hosted_by_msg=hosted_by_msg,
        layout=scope.layout,
    )

    resource = (await sess.execute(
        select(Resource).where(Resource.id == spec["resourceId"])
    )).scalar_one()
    pkg_ctx = {
        "job_id": str(job_id),
        "user_email": spec.get("userEmail", ""),
        "tenant_name": str(resource.tenant_id),
        "resource_name": resource.display_name,
        "scope": f"{scope.layout} - {scope.thread_path}",
        "snapshot_at": job.created_at,
    }

    if await _check_cancelled(sess, job_id):
        await _update_job(sess, job_id, status=JobStatus.CANCELLED)
        await publish(job_id, "cancelled", {})
        return

    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"chat-export-{job_id}-", suffix=".zip")
    os.close(tmp_fd)
    try:
        with open(tmp_path, "wb") as buf:
            pkg = ThreadPackager(
                layout=scope.layout,
                thread_name=scope.thread_path.rsplit("/", 1)[-1],
                thread_path=scope.thread_path,
                format=spec["exportFormat"],
                attachment_source=BlobAttachmentSource(),
            )
            summary = await pkg.write(
                render_messages,
                attachment_map=scope.attachment_map,
                hosted_map=scope.hosted_map,
                out=buf,
                context=pkg_ctx,
            )

        await publish(job_id, "progress", {"stage": "uploading", "percent": 85})
        if await _check_cancelled(sess, job_id):
            await _update_job(sess, job_id, status=JobStatus.CANCELLED)
            await publish(job_id, "cancelled", {})
            return

        account = settings.AZURE_STORAGE_ACCOUNT_NAME
        container = "exports"
        blob_path = f"{job_id}/export.zip"
        shard = azure_storage_manager.get_default_shard()
        up_result = await upload_blob_with_retry_from_file(
            container, blob_path, tmp_path,
            shard=shard, max_retries=3,
        )
        if not up_result.get("success"):
            raise RuntimeError(f"blob upload failed: {up_result.get('error')}")
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # Build the browser-facing download URL through api-gateway →
    # job-service `/api/v1/jobs/export/{job_id}/download`. The endpoint
    # already exists for restore-worker exports; it server-side fetches
    # the blob from internal storage (SeaweedFS / Azure) and streams to
    # the browser with the right Content-Disposition + audit logging.
    #
    # We deliberately drop the raw presigned URL approach:
    #   - SeaweedFS isn't publicly reachable on Railway (the prior
    #     `http://seaweedfs.railway.internal:8333/...` URL the browser
    #     received was unopenable);
    #   - A presigned URL leaks the bytes to anyone who has it — bad
    #     for a backup product carrying customer chat history;
    #   - Going through api-gateway puts the download behind the same
    #     auth + tenant scoping + audit-log path every other download
    #     uses, which is what the existing endpoint at
    #     services/job-service/main.py:2039 already implements.
    #
    # `result.blob_path` + `result.container` mirror the field names the
    # job-service endpoint reads (services/job-service/main.py:2089).
    # Without them the endpoint 404s on the chat-export schema.
    _api_base = (
        os.environ.get("API_GATEWAY_PUBLIC_URL")
        or (
            "https://" + os.environ["RAILWAY_SERVICE_API_GATEWAY_URL"]
            if os.environ.get("RAILWAY_SERVICE_API_GATEWAY_URL")
            else None
        )
        or getattr(settings, "API_GATEWAY_PUBLIC_URL", None)
        or ""
    ).rstrip("/")
    download_url = (
        f"{_api_base}/api/v1/jobs/export/{job_id}/download"
        if _api_base else f"/api/v1/jobs/export/{job_id}/download"
    )

    # Fence the final write against a cancel that landed between the
    # post-upload check and here — an unconditional update would silently
    # overwrite CANCELLING with COMPLETED.
    upd = await sess.execute(
        update(Job)
        .where(Job.id == job_id)
        .where(Job.status != JobStatus.CANCELLING)
        .values(
            status=JobStatus.COMPLETED,
            result={
                "export_zip_blob_path": f"{account}/{container}/{blob_path}",
                "blob_path": blob_path,
                "container": container,
                "signed_url": download_url,
                "total_msgs": len(render_messages),
                "total_bytes": summary["total_bytes"],
                "sha256": summary["sha256"],
            },
        )
    )
    await sess.commit()
    if (upd.rowcount or 0) == 0:
        await _update_job(sess, job_id, status=JobStatus.CANCELLED)
        await publish(job_id, "cancelled", {})
        return
    try:
        await publish(
            job_id, "complete",
            {"url": download_url,
             "sizeBytes": summary["total_bytes"],
             "sha256": summary["sha256"]},
        )
    except Exception:
        # The job IS completed and the row already says so. Letting a Redis
        # blip propagate from here used to flip the committed COMPLETED row
        # back to FAILED, erasing blob_path/container and orphaning the
        # uploaded ZIP. SSE clients miss one event; polling still sees
        # COMPLETED.
        log.warning("completion publish failed id=%s (job remains COMPLETED)",
                    job_id, exc_info=True)
    log.info("job_completed id=%s bytes=%d", job_id, summary["total_bytes"])

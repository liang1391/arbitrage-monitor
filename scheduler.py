"""APScheduler 本地常驻调度器."""

import logging
import signal

from apscheduler.schedulers.blocking import BlockingScheduler

from monitor import run_once

logger = logging.getLogger(__name__)


def run_scheduler(config: dict):
    """Start the APScheduler loop."""
    sources_cfg = config.get("sources", {}).get("smzdm", {})
    interval = sources_cfg.get("poll_interval_sec", 300)

    scheduler = BlockingScheduler()

    def job():
        try:
            run_once(config)
        except Exception:
            logger.exception("Scheduled job failed")

    scheduler.add_job(
        job,
        trigger="interval",
        seconds=interval,
        id="smzdm_poll",
        max_instances=1,
        coalesce=True,
        next_run_time=None,  # Run immediately on start
    )

    # Graceful shutdown
    def shutdown(signum, frame):
        logger.info("Shutting down...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Scheduler started, polling every %ds (Ctrl+C to stop)", interval)
    scheduler.start()

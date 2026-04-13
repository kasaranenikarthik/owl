import logging
import signal
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import settings
from common.metrics import start_metrics_server
from frame_publisher import FramePublisher

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ingest")


def _default_stream_id(source: str) -> str:
    try:
        camera_index = int(source)
        return f"webcam-{camera_index}"
    except ValueError:
        return "stream-0"


def main():
    logger.info("Starting ingest service")
    start_metrics_server(port=8001)

    stream_id = os.environ.get("STREAM_ID") or _default_stream_id(settings.ingest_source)
    publisher = FramePublisher(
        source=settings.ingest_source,
        stream_id=stream_id,
    )

    def handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down", signum)
        publisher.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    publisher.run()


if __name__ == "__main__":
    main()

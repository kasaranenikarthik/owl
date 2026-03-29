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


def main():
    logger.info("Starting ingest service")
    start_metrics_server(port=8001)

    publisher = FramePublisher(
        source=settings.ingest_source,
        stream_id=os.environ.get("STREAM_ID", "stream-0"),
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

import argparse
import asyncio
import traceback

from asterdex.logging_config import get_logger

from config import cleanup_old_logs, load_config, create_config, update_symbols
from service import NotificationService

logger = get_logger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Aster Monitor")
    parser.add_argument(
        "--config", "-c", default="config.toml", help="Config file path"
    )
    parser.add_argument("--webhook", "-w", help="Feishu WebHook URL")
    parser.add_argument("--symbols", "-s", help="Comma-separated symbol list")
    parser.add_argument("--add-symbol", "-a", help="Add symbol(s)")
    parser.add_argument("--remove-symbol", "-r", help="Remove symbol(s)")
    parser.add_argument(
        "--list-symbols", "-l", action="store_true", help="List symbols"
    )
    args = parser.parse_args()

    if args.webhook:
        single = args.symbols.split(",") if args.symbols else None
        create_config(args.config, args.webhook, single)
    elif args.add_symbol:
        update_symbols(
            args.config, "add", [s.strip().upper() for s in args.add_symbol.split(",")]
        )
        return
    elif args.remove_symbol:
        update_symbols(
            args.config,
            "remove",
            [s.strip().upper() for s in args.remove_symbol.split(",")],
        )
        return
    elif args.list_symbols:
        config = load_config(args.config)
        sym = config.get("symbols", {})
        print(f"single_list: {sym.get('single_list', [])}")
        print(f"pair_list: {sym.get('pair_list', [])}")
        return

    cleanup_old_logs()
    service = NotificationService(args.config)
    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, stopping...")
    except Exception as e:
        logger.error(f"Main error: {e}")
        await service.send_error(e, f"Main\n{traceback.format_exc()}")
    finally:
        await service.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()

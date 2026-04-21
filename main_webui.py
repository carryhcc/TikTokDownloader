from asyncio import CancelledError, run
from threading import Timer
from webbrowser import open as open_browser

from src.application import TikTokDownloader
from src.application.main_server import APIServer
from src.custom import SERVER_HOST, SERVER_PORT


async def main():
    async with TikTokDownloader() as downloader:
        downloader.check_config()
        await downloader.check_settings(False)

        url = f"http://127.0.0.1:{SERVER_PORT}/comment-center"
        Timer(1.2, lambda: open_browser(url)).start()

        try:
            await APIServer(
                downloader.parameter,
                downloader.database,
            ).run_server(
                SERVER_HOST,
                SERVER_PORT,
            )
        except (KeyboardInterrupt, CancelledError):
            return


if __name__ == "__main__":
    run(main())

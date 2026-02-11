import logging

from src.task import Etl

logging.basicConfig(level=logging.WARNING)


def main() -> None:
    Etl().run()


if __name__ == "__main__":
    main()

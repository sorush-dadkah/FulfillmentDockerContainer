import pandas as pd


def pandas_format() -> None:
    pd.options.display.width = 0
    pd.set_option("display.max_rows", None)

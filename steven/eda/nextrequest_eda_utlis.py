"""
Functions useful for performing EDA on data scraped by the NextRequest webscraper.
"""

import pandas as pd
from io import StringIO


def nextrequest_df_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df_fillna(df)

    df['docs_df'] = df['docs'].apply(csv_to_df)
    df['docs_df'] = df['docs_df'].apply(df_fillna)
    df['docs_df'] = df['docs_df'].apply(remove_empty_df)

    df['msgs_df'] = df['msgs'].apply(csv_to_df)
    df['msgs_df'] = df['msgs_df'].apply(df_fillna)

    # Split

    return df


def df_fillna(df: pd.DataFrame) -> pd.DataFrame:
    return df.convert_dtypes().fillna('') if (df is not None) else None


def csv_to_df(csv: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(csv)) if csv else None


def remove_empty_df(df: pd.DataFrame) -> pd.DataFrame:
    return None if ((df is None) or (type(df) == str and not df) or df.empty) else df


def convert_time_to_dt(df: pd.DataFrame, col: str = 'date') -> pd.DataFrame:
    return df.assign(**{col + '_dt': pd.to_datetime(df[col])})


def extract_time(df: pd.DataFrame, col: str = 'date', on: str = ' via ') -> pd.DataFrame:
    return df_fillna(df.join(
            df[col].str.split(on, expand=True)
        ).drop(
            columns=col
        ).rename(
            columns={0: col, 1: on.strip()}
        )) if (df is not None) else None


def melt_depts(df: pd.DataFrame) -> pd.DataFrame:
    df_depts = df.join(df['depts'].str.split(', ', expand=True))  # Split departments into columns
    return df_depts.melt(  # Melt on the individual departments
            id_vars=df.columns
        )[
            lambda df: df['value'].notna()  # Drop all NA values
        ].drop(  # Drop the variable column, rename the value column, and reset indices
            columns='variable'
        ).rename(
            columns={'value': 'dept'}
        ).reset_index().drop(
            columns='index'
        )

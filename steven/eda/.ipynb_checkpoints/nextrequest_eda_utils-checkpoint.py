"""
Functions useful for performing EDA on data scraped by the NextRequest webscraper.
"""
import pandas as pd
from io import StringIO


def nextrequest_df_clean(df, debug=False):
    """
    Prepare a DataFrame of NextRequest requests for pandas-based EDA
    """
    # Fill NA values
    df = df_fillna(df)
    if debug: print('fillna complete')

    # Convert docs CSV column into docs_df DataFrame column
    df['docs_df'] = df['docs'].apply(
            lambda csv: remove_empty_df(df_fillna(csv_to_df(csv)))
        )
    if debug: print('docs_df complete')
    
    # Convert msgs CSV column into msgs_df DataFrame column
    df['msgs_df'] = df['msgs'].apply(
            lambda csv: df_fillna(csv_to_df(csv))
        )
    if debug: print('msgs_df complete')

    # Extract times from requests and convert to datetime
    df = convert_time_to_dt(extract_time(df, col='date', on='via', 
                                         re=True, pattern=r'([a-zA-Z]* \d{1,2}, \d{,5}) ([a-zA-Z ]*)'), 
                            col='date')
    if debug: print('date-via split complete')
    
    # Extract times from messages and convert to datetime
    df['msgs_df'] = df['msgs_df'].apply(
            lambda df: convert_time_to_dt(extract_time(df, col='time', on=' by '), col='time')
        )
    if debug: print('time-by split in msgs complete')

    return df


def csv_to_df(csv):
    """
    Convert a CSV string into a DataFrame
    """
    return pd.read_csv(StringIO(csv)) if csv else None


def df_fillna(df):
    """
    Fill empty value with a blank string, then inferentially convert column data types
    """
    return df.convert_dtypes().fillna('') if (df is not None) else None


def remove_empty_df(df):
    """
    Set empty DataFrames to None
    """
    return None if ((df is None) or (type(df) == str and not df) or df.empty) else df


def extract_time(df, col='time', on=' by ', re=False, pattern=''):
    """
    Extract time strings from a column of a NextRequest DataFrame. Can use either splitting on a string or regex extraction
    """
    # TODO: '[DATE] in person' is causing problems, find a simple way to extract the date from that. Maybe try regex?
    if re:
        df_extract = df[col].str.extract(pattern)
    else:
        df_extract = df[col].str.split(on, expand=True)

    return df_fillna(df.join(
                df_extract
            ).drop(
                columns=col
            ).rename(
                columns={0: col, 1: on.strip()}
            )) if (df is not None) else None


def convert_time_to_dt(df, col='date'):
    """
    Convert a column of time strings into datetime
    """
    return df.assign(**{col + '_dt': pd.to_datetime(df[col])})


def melt_depts(df):
    """
    Melt a NextRequest DataFrame on the departments assigned to each entry, with one row for each department in each entry
    """
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


def get_open_time(msgs):
    """
    Get the request open time from the messages
    """
    if msgs is None: return None
    
    request_opened = msgs[lambda df: df['title'].str.contains('Opened')].sort_values(by='time_dt', ignore_index=True)
    if request_opened.empty:
        request_opened = msgs[lambda df: df['title'].str.contains('Published')].sort_values(by='time_dt', ignore_index=True)
    return request_opened.loc[0]['time_dt']


def get_close_time(msgs, get_all=False):
    """
    Get the request close time(s) from the messages. 
    """
    if msgs is None: return None

    request_closed = msgs[lambda df: df['title'].str.contains('Closed')].sort_values(by='time_dt', 
                                                                                     ascending=False, ignore_index=True)
    if request_closed.empty: return None
    if get_all: return list(request_closed['time_dt'].to_numpy())
    return request_closed.loc[0]['time_dt']

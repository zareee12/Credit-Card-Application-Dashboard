import pandas as pd

def read_csv():
    """
    Read CSV data.
    """
    csv_file = "c:/DE Project/tes/application_record.csv"
    df = pd.read_csv(csv_file)
    return df

def format_standarization(df):
    """
    Perform format standardization.
    """
    df_transform1 = df.copy()
    df_transform1['YEARS_BIRTH'] = abs(df_transform1['DAYS_BIRTH'] / 365.25).astype(int)
    df_transform1['YEARS_EMPLOYED'] = abs(df_transform1['DAYS_EMPLOYED'] / 365.25).astype(int)
    df_transform1.drop(columns=['DAYS_BIRTH', 'DAYS_EMPLOYED'], inplace=True)
    df_transform2 = df_transform1.copy()
    df_transform2['CODE_GENDER'] = df_transform2['CODE_GENDER'].apply(lambda x: 1 if x == 'F' else 0)
    df_transform2['FLAG_OWN_CAR'] = df_transform2['FLAG_OWN_CAR'].apply(lambda x: 1 if x == 'Y' else 0)
    df_transform2['FLAG_OWN_REALTY'] = df_transform2['FLAG_OWN_REALTY'].apply(lambda x: 1 if x == 'Y' else 0)
    return df_transform2

def cleanse_null_data(df):
    """
    Cleanse null data.
    """
    df_transform3 = df.dropna(how='all')
    return df_transform3

def save_data(df):
    """
    Save cleansed data to a new CSV file.
    """
    save_path = "c:/DE Project/tes/application_record_cleansed.csv"
    df.to_csv(save_path, index=False)

# Read CSV
df = read_csv()

# Perform format standardization
df_transformed = format_standarization(df)

# Cleanse null data
df_cleaned = cleanse_null_data(df_transformed)

# Save cleansed data
save_data(df_cleaned)

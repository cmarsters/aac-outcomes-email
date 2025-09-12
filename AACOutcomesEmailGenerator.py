import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

############################### FUNCTIONS ######################################
def getdates(tminus:int):
    # get yesterday's date:
    yesterday = datetime.now() - timedelta(days=tminus) # tminus = 1 --> yesterday
    start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()

    # Get 30 days from yesterday (AAC return window):
    thirty_days_prior = yesterday - timedelta(days=30)
    start_of_30daysprior = thirty_days_prior.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    return yesterday, start_of_day, end_of_day, start_of_30daysprior


def getOutcomes(start_datetime, end_datetime):

    # pull outcome data from database:
    OUTCOMES_API = "https://data.austintexas.gov/resource/gsvs-ypi7.json"
    params = { # yesterday's outcome data
        "$where": f"outcome_date between '{start_datetime}' and '{end_datetime}'",
        "$limit": 5000
    }
    response = requests.get(OUTCOMES_API, params=params)
    data = response.json()
    df = pd.DataFrame(data) # this df contains all outcomes data

    # Desired columns to ensure are present:
    expected_columns = [
        'outcome_status', 'type', 'name', 'animal_id',
        'primary_breed', 'days_in_shelter', 'date_of_birth','outcome_date','euthanasia_reason'
    ]

    # Add any missing columns as empty strings
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ''

    # Unify 'adopted altered'/'adopted unaltered'/'adopted' outcomes:
    df['outcome_status'] = df['outcome_status'].str.lower().replace({
        'adopted altered': 'adopted',
        'adopted unaltered': 'adopted',
        'adopted offsite(altered)': 'adopted offsite',
        'adopted offsite(unaltered)': 'adopted offsite'
    }).str.capitalize()

    return df


def format_age(row, decimals: int = 1, ref_col: str = 'outcome_date'):
    """
    Compute age (in years) from date_of_birth.
    - Uses the row's outcome_date (or another ref_col) as the 'as of' date when present,
      otherwise falls back to the current time.
    - Returns a float rounded to `decimals`, or '' if we can't compute a valid age.
    """
    dob = pd.to_datetime(row.get('date_of_birth'), errors='coerce')
    ref = pd.to_datetime(row.get(ref_col), errors='coerce')

    # Fall back to "now" if no reference date in the row
    if pd.isna(ref):
        ref = pd.Timestamp.now(tz=None)

    # If no DOB or DOB is after reference date, leave blank
    if pd.isna(dob) or dob > ref:
        return ''

    years = (ref - dob).days / 365.2425  # mean tropical year
    return round(years, decimals)



def formatSpeciesDF(df):
    df = df.copy()  # prevents SettingWithCopyWarning

    # Create a readable age column
    df['age'] = df.apply(format_age, axis=1)

    # Trim down to columns of interest:
    columns = ['outcome_status', 'type', 'name', 'animal_id', 'primary_breed', 'age', 'days_in_shelter', 'euthanasia_reason']
    df = df[columns]

    # Rename columns so html output is more readable:
    df.rename(columns={
        'outcome_status': 'Outcome',
        'type': 'Species',
        'name':'Name',
        'animal_id':'ID',
        'primary_breed':'Primary Breed',
        'age':'Age (Years)',
        'days_in_shelter':'Days in Shelter',
        'euthanasia_reason':'Euthanasia Reason'}, inplace=True)


    # Replace NaNs with empty strings:
    df = df.fillna('')

    # Custom outcome order: everything except "Returned", which goes last
    outcomes_present = df['Outcome'].unique().tolist()
    outcome_order = sorted([o for o in outcomes_present if o != 'Returned to AAC']) + ['Returned to AAC']

    # Only apply categorical if Outcome is not empty
    if df['Outcome'].ne('').any():
        df['Outcome'] = pd.Categorical(df['Outcome'], categories=outcome_order, ordered=True)
    df = df.sort_values(by='Outcome')

    return df

def highlight_returns_in_html(html_str):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_str, "html.parser")

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if cells and cells[0].text.strip().lower() == "returned to aac":
            row['style'] = "background-color: #f8d7da;"  # light red

    return str(soup)

################################################################################
################################################################################
################################################################################



## Get relevant dates/times
yesterday, start_of_day, end_of_day, start_30d = getdates(tminus=1)


## Pull outcome data from database
df = getOutcomes(start_of_day, end_of_day)
outcomes_30d = getOutcomes(start_30d, end_of_day)

## Filter outcomes_30d to adopted dogs only
df_30 = outcomes_30d[
    (outcomes_30d['outcome_status'].str.lower().str.contains('adopted'))
].copy()

# for dogs who have been adopted and returned more than once,
# keep only most recent adoption:
df_30 = (
    df_30.sort_values(by='outcome_date', ascending=False)
    .drop_duplicates(subset='animal_id', keep='first')
)

## Look for adoption returns
# Look in intake data for pets returned yesterday
INTAKE_API = "https://data.austintexas.gov/resource/pyqf-r2dc.json"
intake_params = {
    "$where": f"source_date between '{start_of_day}' and '{end_of_day}'",
    "$limit": 5000
}
intake_data = requests.get(INTAKE_API, params=intake_params).json()
intake_df = pd.DataFrame(intake_data)

# Desired columns to ensure are present:
expected_columns = [
    'source_date', 'animal_id', 'type', 'source_name',
    'name_at_intake', 'ispreviouslyspayedneutered', 'sex', 'primary_breed',
    'primary_color', 'secondary_color', 'intake_health_condition'
]


# Add any missing columns as empty strings
for col in expected_columns:
    if col not in intake_df.columns:
        intake_df[col] = ''

# Rename columns so html output is more readable:
intake_df.rename(columns={
'source_date': 'intake_date',
'source_name': 'intake_status'}, inplace=True)

# Filter for dog returns
returns_df = intake_df[
    (intake_df.get('intake_status', '').str.lower() == 'returns')
].copy()


# Convert dates
returns_df['intake_date'] = pd.to_datetime(returns_df['intake_date'])
df_30['outcome_date'] = pd.to_datetime(df_30['outcome_date'])



# Merge to get original name from outcomes
returns_df = returns_df.merge(
    df_30[['animal_id', 'name', 'outcome_date', 'days_in_shelter', 'date_of_birth', 'euthanasia_reason']],
    how='left',
    on='animal_id',
    suffixes=('prev_', '')
)


# Integrate "Returns" as an outcome type
returns_df['outcome_status'] = 'Returned to AAC'
returns_df['days_in_shelter'] = ''
returns_df['age'] = returns_df.apply(format_age, axis=1)

df['outcome_status'] = df['outcome_status'].replace({
    'Doa': 'DOA'
})

if not returns_df.empty:
    df_withReturns = pd.concat([df, returns_df], ignore_index=True)
    df_withReturns.fillna('', inplace=True)
else:
    df_withReturns = df

## Verify and organize outcome data:
if not df.empty and 'outcome_status' in df.columns:

    # Filter for dogs and puppies
    dog_df = df_withReturns[df_withReturns['type'].str.lower().isin(['dog', 'puppy'])].copy()

    # Filter for cats and kittens
    cat_df = df_withReturns[df_withReturns['type'].str.lower().isin(['cat', 'kitten'])].copy()

    # Filter for all other outcomes
    df_withReturns['type_clean'] = df_withReturns['type'].str.lower().fillna('') # lowercase type col.
    excluded_types = ['dog', 'puppy', 'cat', 'kitten']
    other_df = df_withReturns[~df_withReturns['type_clean'].isin(excluded_types)].copy()
    other_df.drop(columns='type_clean', inplace=True)

    # Rename columns so html output is more readable:
    dog_df, cat_df, other_df = formatSpeciesDF(dog_df), formatSpeciesDF(cat_df), formatSpeciesDF(other_df)

    # Create summary sections for each species:
    dog_counts = dog_df['Outcome'].value_counts()
    cat_counts = cat_df['Outcome'].value_counts()
    other_counts = other_df['Outcome'].value_counts()

    # Combine summaries for each species into one DataFrame
    summary_df = pd.DataFrame({
        'Dog/Puppy': dog_counts,
        'Cat/Kitten': cat_counts,
        'Other': other_counts
    }).fillna(0).astype(int)

    # Add a 'Total' column
    summary_df['Total'] = summary_df.sum(axis=1)

    # Move 'Returned to AAC' to bottom, if present
    if 'Returned to AAC' in summary_df.index:
        returned_row = summary_df.loc[['Returned to AAC']]
        summary_df = summary_df.drop(index='Returned to AAC')
        summary_df = pd.concat([summary_df, returned_row])

    # Convert to HTML
    summary_df.index.name = None
    species_summary_html = summary_df.to_html(border=1, justify='center')

    # Highlight 'Returned to AAC' in red
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(species_summary_html, "html.parser")

    for row in soup.find_all("tr"):
        th = row.find("th")
        if th and th.text.strip().lower() == "returned to aac":
            row['style'] = "background-color: #f8d7da;"  # light red background

    species_summary_html = str(soup)

    # Create detailed sections for each species:
    dog_html = dog_df.to_html(index=False, border=1, justify='center')
    cat_html = cat_df.to_html(index=False, border=1, justify='center')
    other_html = other_df.to_html(index=False, border=1, justify='center')

    dog_html = highlight_returns_in_html(dog_df.to_html(index=False, border=1, justify='center'))
    cat_html = highlight_returns_in_html(cat_df.to_html(index=False, border=1, justify='center'))
    other_html = highlight_returns_in_html(other_df.to_html(index=False, border=1, justify='center'))


    # Build the HTML email body
    html_body = f"""
    <html>
    <head>
    <style>
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 30px;
            font-family: Arial, sans-serif;
            font-size: 14px;
            table-layout: auto;
        }}
        th {{
            background-color: #f2f2f2;
            padding: 2px 4px;
            text-align: center;
            border: 0px solid #ddd;
        }}
        td {{
            padding: 2px 4px;
            text-align: center;
            border: 0px solid #ddd;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        h2, h3 {{
            font-family: Arial, sans-serif;
        }}
    </style>
    </head>
    <body>
    <h2>Austin Animal Center Outcomes for {yesterday.date()}</h2>

    <h3>Summary of Outcomes for {yesterday.date()}:</h3>
    {species_summary_html}

    <h3>Dog Outcome Details for {yesterday.date()}:</h3>
    {dog_html}

    <h3>Cat Outcome Details for {yesterday.date()}:</h3>
    {cat_html}

    <h3>Other Species Outcome Details for {yesterday.date()}:</h3>
    {other_html}
    </body>
    </html>
    """



else:
    html_body = f"No outcome records were found for {yesterday.date()}."
    print(f"No outcome records were found for {yesterday.date()}.")


## Create and send outcomes email:

# Email configuration
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_ADDRESS = os.getenv('AAC_GMAIL')
EMAIL_PASSWORD = os.getenv('AAC_GMAIL_PW')
RECIPIENT_EMAIL = 'celenamarsters@gmail.com, aac-outcomes@googlegroups.com'

if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
    raise ValueError("Missing email credentials — check environment variables.")


# Compose the email
msg = MIMEMultipart("alternative")
msg['Subject'] = f"Austin Animal Center Outcomes for {yesterday.date()}"
msg['From'] = EMAIL_ADDRESS
msg['To'] = RECIPIENT_EMAIL

# Attach the HTML version
html_part = MIMEText(html_body, "html")
msg.attach(html_part)

# Send the email
with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
    server.starttls()
    server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    server.send_message(msg, to_addrs=RECIPIENT_EMAIL.split(', '))


print('email(s) sent.')

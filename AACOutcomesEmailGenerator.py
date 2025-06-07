import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

## Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()

## Pull outcome data from database
API_ENDPOINT = "https://data.austintexas.gov/resource/gsvs-ypi7.json"
params = {
    "$where": f"outcome_date between '{start_of_day}' and '{end_of_day}'",
    "$limit": 5000
}

response = requests.get(API_ENDPOINT, params=params)
data = response.json()
df = pd.DataFrame(data) # this df contains all outcomes data from the prev. day
# print(df.head())

# Desired columns to ensure are present:
expected_columns = [
    'outcome_status', 'type', 'name', 'animal_id',
    'primary_breed', 'age_years', 'age_months', 'age_weeks',
    'euthanasia_reason'
]

# Add any missing columns as empty strings
for col in expected_columns:
    if col not in df.columns:
        df[col] = ''

# print(df.columns)

## Verify and organize data:
if not df.empty and 'outcome_status' in df.columns:
    # outcome_summary = df['outcome_status'].value_counts()

    # Filter for dogs and puppies
    dog_df = df[df['type'].str.lower().isin(['dog', 'puppy'])].copy()

    # Filter for cats and kittens
    cat_df = df[df['type'].str.lower().isin(['cat', 'kitten'])].copy()

    # Filter for all other species
    df['type_clean'] = df['type'].str.lower().fillna('') # lowercase type col.
    excluded_types = ['dog', 'puppy', 'cat', 'kitten']
    other_df = df[~df['type_clean'].isin(excluded_types)].copy()
    other_df.drop(columns='type_clean', inplace=True)


    # Create a readable age column
    def format_age(row):
        try:
            y = int(pd.to_numeric(row.get('age_years'), errors='coerce') or 0)
            m = int(pd.to_numeric(row.get('age_months'), errors='coerce') or 0)
            w = int(pd.to_numeric(row.get('age_weeks'), errors='coerce') or 0)
        except Exception:
            y = m = w = 0

        parts = []
        if y: parts.append(f"{y}y")
        if m: parts.append(f"{m}m")
        if w and not y and not m:
            parts.append(f"{w}w")
        return ' '.join(parts) if parts else "Unknown"


    dog_df['age'] = dog_df.apply(format_age, axis=1)
    cat_df['age'] = cat_df.apply(format_age, axis=1)
    other_df['age'] = other_df.apply(format_age, axis=1)

    # Trim down to columns of interest:
    columns = ['outcome_status', 'type', 'name', 'animal_id', 'primary_breed', 'age', 'euthanasia_reason']

    dog_df = dog_df[columns]
    cat_df = cat_df[columns]
    other_df = other_df[columns]

    # Rename columns so html output is more readable:
    dog_df.rename(columns={
    'outcome_status': 'Outcome',
    'type': 'Species',
    'name':'Name',
    'animal_id':'ID',
    'primary_breed':'Primary Breed',
    'age':'Age',
    'euthanasia_reason':'Euthanasia Reason'}, inplace=True)

    cat_df.rename(columns={
    'outcome_status': 'Outcome',
    'type': 'Species',
    'name':'Name',
    'animal_id':'ID',
    'primary_breed':'Primary Breed',
    'age':'Age',
    'euthanasia_reason':'Euthanasia Reason'}, inplace=True)

    other_df.rename(columns={
    'outcome_status': 'Outcome',
    'type': 'Species',
    'name':'Name',
    'animal_id':'ID',
    'primary_breed':'Primary Breed',
    'age':'Age',
    'euthanasia_reason':'Euthanasia Reason'}, inplace=True)

    # Sort by outcome status:
    dog_df = dog_df.sort_values(by='Outcome')
    cat_df = cat_df.sort_values(by='Outcome')
    other_df = other_df.sort_values(by='Outcome')

    # Replace NaNs with empty strings:
    dog_df = dog_df.fillna('')
    cat_df = cat_df.fillna('')
    other_df = other_df.fillna('')

    # Create summary sections for each species:
    # dog_outcome_summary = dog_df['Outcome'].value_counts()
    # dog_summary_html = dog_outcome_summary.to_frame().to_html(header=False, border=0)
    #
    # cat_outcome_summary = cat_df['Outcome'].value_counts()
    # cat_summary_html = cat_outcome_summary.to_frame().to_html(header=False, border=0)
    #
    # other_outcome_summary = other_df['Outcome'].value_counts()
    # other_summary_html = other_outcome_summary.to_frame().to_html(header=False, border=0)

    dog_counts = dog_df['Outcome'].value_counts()
    cat_counts = cat_df['Outcome'].value_counts()
    other_counts = other_df['Outcome'].value_counts()

    # Combine into one DataFrame
    summary_df = pd.DataFrame({
        'Dog/Puppy': dog_counts,
        'Cat/Kitten': cat_counts,
        'Other': other_counts
    }).fillna(0).astype(int)

    # Optional: Add a 'Total' column
    summary_df['Total'] = summary_df.sum(axis=1)

    # Convert to HTML
    summary_df.index.name = None  # Prevents "Outcome" row in HTML table
    species_summary_html = summary_df.to_html(border=1, justify='center')


    # Create detailed sections for each species:
    dog_html = dog_df.to_html(index=False, border=1, justify='center')
    cat_html = cat_df.to_html(index=False, border=1, justify='center')
    other_html = other_df.to_html(index=False, border=1, justify='center')

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
    body = f"No outcome records were found for {yesterday.date()}."


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
subject = f"Austin Animal Center Outcomes for {yesterday}" ###########


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

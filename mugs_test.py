import json as json
import pandas as pd
from datetime import datetime

def iso_to_unix(iso_string):
    dt = datetime.fromisoformat(iso_string)
    return int(dt.timestamp())

# Part 1 

df1 = pd.read_json('Patient.ndjson', lines=True, orient='records')
df1.to_json('Patient.ndjson', lines=True, orient='records')

# print(df1)
# print(df1.columns)

df2 = pd.read_json('Condition.ndjson', lines=True, orient='records')
df2.to_json('Condition.ndjson', lines=True, orient='records')

# print(df2)
# print(df2.columns)

df3 = pd.read_json('Encounter.ndjson', lines=True, orient='records')
df3.to_json('Encounter.ndjson', lines=True, orient='records')

# print(df3)
# print(df3.columns)

df4 = pd.read_json('EncounterICU.ndjson', lines=True, orient='records')
df4.to_json('EncounterICU.ndjson', lines=True, orient='records')

# print(df4)
# print(df4.columns)

# Part 2

patient_conditions = {}

for patient_id in df1['id']:
    matching_rows = df2[df2['subject'].apply(lambda x: x['reference'].split('/')[-1]) == patient_id]
    conditions = matching_rows['code'].apply(lambda x: x['coding'][0]['code']).tolist()
    patient_conditions[patient_id] = conditions


for patient_id, conditions in patient_conditions.items():
    print(f"Patient ID: {patient_id}, Conditions: {conditions}")

# Part 3

patient_data = []

for patient_id in df1['id']:
    matching_rows = df2[df2['subject'].apply(lambda x: x['reference'].split('/')[-1]) == patient_id]
    
    for index, row in matching_rows.iterrows():
        code = row['code']['coding'][0]['code']
        display = row['code']['coding'][0]['display']
        encounter_id = row['encounter']['reference'].split('/')[-1]

        # Part 4

        start_time = None
        for df in [df3, df4]:
            matching_row = df[df['id'] == encounter_id].iloc[0]
            if not pd.isnull(matching_row['period']):
                start_time = matching_row['period']['start']
                start_time_unix = iso_to_unix(start_time)
                break

        patient_data.append({'pid': patient_id,'time': start_time_unix, 'code': code, 'description': display})

patient_df = pd.DataFrame(patient_data)

# print(patient_df)

patient_df.to_csv("PatientData.csv", index=False)
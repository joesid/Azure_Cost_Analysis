import pandas as pd
import glob

file_pattern = '*.csv'

# Specify the column names manually
column_names = ['DATE', 'SERVICE PERIOD', 'TRANSACTION TYPE', 'PRODUCT FAMILY', 'PRODUCT TYPE', 'PRODUCT SKU', 'INVOICE SECTION', 'CHARGES/CREDITS CURRENCY', 'CHARGES /CREDITS', 'TOTAL CURRENCY', 'TOTAL']

# Initialize an empty DataFrame to store the aggregated results
aggregated_data = pd.DataFrame()

# Loop through all the CSV files in the directory
for file in glob.glob(file_pattern):
    # Read the CSV file into a DataFrame without a header, set the column names manually, and skip rows with too many fields
    df = pd.read_csv(file, header=None, names=column_names, on_bad_lines='skip')
    print(f"Processing file: {file}")
    print(f"Columns: {df.columns.tolist()}")

    # Extract the unique service period values from the 'SERVICE PERIOD' column
    service_periods = df['SERVICE PERIOD'].unique()

    # Iterate over each service period
    for service_period in service_periods:
        # Filter the DataFrame for the current service period
        df_filtered = df[df['SERVICE PERIOD'] == service_period]

        # Calculate the total charges/credits for each product family
        product_family_totals = df_filtered.groupby('PRODUCT FAMILY')['CHARGES /CREDITS'].sum()

        # Calculate the total charges/credits for all rows
        total_charges = df_filtered['TOTAL'].sum()

        # Create a new DataFrame with the aggregated results for this service period
        result = pd.DataFrame({
            'SERVICE PERIOD': [service_period],
            'STORAGE': [product_family_totals.get('Storage', 0)],
            'NETWORK': [product_family_totals.get('Networking', 0)],
            'COMPUTE': [product_family_totals.get('Compute', 0)],
            'TOTAL': [total_charges]
        })

        # Append the results to the aggregated_data DataFrame
        aggregated_data = pd.concat([aggregated_data, result], ignore_index=True)

# Save the aggregated results to a new CSV file
aggregated_data.to_csv('aggregated_results.csv', index=False)
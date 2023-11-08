import argparse
import numpy as np
import pandas as pd
from bisect import bisect
from pyjstat import pyjstat

# Redacted. URLs and dataframes (first_df, second_df, third_df) had more descriptive names.
# Most of the field names have been shortened as well.
FIRST_URL = ''
SECOND_URL = ''
THIRD_URL = ''

parser = argparse.ArgumentParser(description='Download, process, and analyze data.')
parser.add_argument('-y', '--year', type=str, help='year to perform the analysis on')
args = parser.parse_args()

### FIRST URL
# Gather a list of countries that are marked with a keyword and then remove them from dataframe
# Convert to float and fill NaNs with interpolation
first_df = pd.read_csv(FIRST_URL, delimiter='|', decimal=',')
list_of_countries = first_df[first_df.apply(lambda r: r.str.contains('keyword', case=False).any(), axis=1)]['Country'].tolist()
first_df = first_df[~first_df['Country'].isin(list_of_countries)]
first_df = first_df.set_index('Country')
first_df = first_df.interpolate(axis=1)

# Use this later to omit countries that are not in FIRST_URL
countries_in_first_dataset = first_df.index.values.tolist()


### SECOND URL
# Remove unnecessary countries, rows, and columns
# Pivot the table and fill NaNs with interpolation
second_dataset = pyjstat.Dataset.read(SECOND_URL)
second_df = second_dataset.write('dataframe')
second_df = second_df[second_df['Class'] == 'Total']
second_df = second_df[~second_df['Country name'].isin(list_of_countries)]
second_df = second_df[second_df['Country name'].isin(countries_in_first_dataset)]
second_df = second_df.drop(columns=['Frequency',
                              'Class'])
second_df = second_df.pivot(index='Country name', columns='Time', values='value')
second_df = second_df.interpolate(axis=1)


### THIRD URL
# Remove unnecessary countries and columns
# Pivot the table, add column for a given year and fill NaNs with interpolation
third_dataset = pyjstat.Dataset.read(THIRD_URL)
third_df = third_dataset.write('dataframe')
third_df = third_df[~third_df['Country name'].isin(list_of_countries)]
third_df = third_df[third_df['Country name'].isin(countries_in_first_dataset)]
third_df = third_df.drop(columns=['Frequency',
                                  'Size',
                                  'Class',
                                  'Indicator',
                                  'Unit'])
third_df = third_df.pivot(index='Country name', columns='Time', values='value')

# As the columns are a non-continuous but sorted list of years, find the column index where to insert this newly derived year
column_values = list(third_df.columns.values)
column_values = [int(x) for x in column_values]
insert_index = bisect(column_values, int(args.year))
third_df.insert(loc=insert_index, column=args.year, value=np.nan)
third_df = third_df.interpolate(axis=1)


# This analysis is done only with the data from the given year, so omit every country that is still NaN in the given year.
# As the NaN values are filled forward via the interpolation method, this is the case ONLY when there is no data or the earliest data point is in 'given year + 1' at the earliest.
list_of_nan_countries = second_df.loc[second_df[args.year].isnull()].index.values.tolist()
list_of_nan_countries.extend(third_df.loc[third_df[args.year].isnull()].index.values.tolist())
list_of_nan_countries.extend(first_df.loc[first_df[args.year].isnull()].index.values.tolist())

first_df = first_df.drop(index=list_of_nan_countries)
second_df = second_df.drop(index=list_of_nan_countries)
third_df = third_df.drop(index=list_of_nan_countries)

# Provides the analysis result for the assignment
print((first_df[args.year] * second_df[args.year] * third_df[args.year]).round(2).sort_values(ascending=False))

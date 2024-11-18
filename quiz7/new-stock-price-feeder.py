#!/usr/bin/env python3
import sys, time
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: new-stock-price-feeder.py <stock data file path>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)

    # set up dataframe of stock data    
    file_path = sys.argv[1]
    combined = pd.read_csv(file_path)

    aapl_df = combined[combined['Symbol'] == "AAPL"].set_index('datatime')
    msft_df = combined[combined['Symbol'] == "MSFT"].set_index('datetime')

    aapl_df.sort_index(axis=0, inplace=True)
    msft_df.sort_values(axis=0, inplace=True)

    max_rows = msft_df.shape[0]

    # Walk down MSFT stock dataframe. (This had more rows than AAPL stock data).
    # Print MSFT and AAPL stock data for datetimes that exist in both
    for i in range(max_rows):
        index = msft_df.iloc[i].name
        
        if index in aapl_df.index:
            msft_price = msft_df.loc[index]['open']
            aapl_price = aapl_df.loc[index]['open']

            print('%19s\t%.4f\t%.4f' % (index, msft_price, aapl_price))



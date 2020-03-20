import pandas as pd


def main():

    # change this variable to use the code for other years
    year = 2003

    # gets and saves the stock_prices for the given year
    stock_prices = get_and_clean_data(
        filename=f"stock_prices_{year}",
        isin_column="International Security Identification Number",
        other_columns=["Data Date - Daily Prices", "Shares Outstanding", "Price - Close - Daily"],
        year=year,
    )
    save_to_excel_file(stock_prices, filename=f"stock_prices_{year}_clean")

    # gets and saves the esg_ratings for the given year
    esg_ratings = get_and_clean_data(
        filename=f"esg_ratings_{year}",
        isin_column="Company ISIN",
        other_columns=[f"ESG {year}"],
        year=year
    )
    save_to_excel_file(esg_ratings, filename=f"esg_ratings_{year}_clean")

    # merges stock_prices and esg_ratings and saves the data
    merged_data_set = merge_data_sets(dataset_one=stock_prices, dataset_two=esg_ratings)
    save_to_excel_file(merged_data_set, filename=f"merged_data_{year}")


def get_and_clean_data(filename, isin_column, other_columns, year):
    """ Gets the data from the given filename and removes unnecessary and unwanted data """

    data = get_data(
        file=filename, isin_column=isin_column, other_columns=other_columns,
    )

    data = remove_companies_without_valid_isin(data, isin_column)

    if "stock_prices" in filename:
        dates = get_relevant_dates(year)
        data = get_only_matching_rows(data=data, column="Data Date - Daily Prices", matching_values=dates)

    return data


def get_relevant_dates(year):
    """ Gets the relevant dates for the given year """

    if year == 2003:
        return [
            "20030102", "20030203", "20030303", "20030401", "20030502", "20030603", "20030701", "20030801", "20030902",
            "20031001", "20031103", "20031201",
        ]
    # you can add additional elif-clauses here if needed. f. ex:
    # elif year == 2004:
    #     return ....
    else:
        raise ValueError("A list of relevant dates does not exist for the given year")


def get_only_matching_rows(data, column, matching_values):
    """
    Gets all rows which match against one of the values in the list matching_values in the given column.
    All other rows are discarded
    """

    if type(matching_values) != list:
        raise ValueError("matching_values should be a list")

    old_length = len(data[column])

    data = data[data[column].isin(matching_values)]

    new_length = len(data[column])
    percentage = 1.0 - new_length / old_length
    print(f"Removed irrelevant rows (reduction of {percentage:.2%})\n")

    return data


def remove_companies_without_valid_isin(data, isin_column):
    """ Removes any row without a ISIN number. """

    old_length = len(data[isin_column])

    data.dropna(subset=[isin_column], inplace=True)

    new_length = len(data[isin_column])
    percentage = 1.0 - new_length / old_length

    print(
        f"Removed {old_length - new_length} records without valid ISIN (reduction of {percentage:.2%})"
    )

    return data


def merge_data_sets(dataset_one, dataset_two):
    """
    Will join the data sets according ISIN. Only ISIN-values that appear in both data sets will be joined.
    ISIN-values that only appear in one of the two sets will be discarded.
    """

    print(f"Merging {dataset_one} with {dataset_two}")
    merged_dataset = pd.merge(
        left=dataset_one,
        right=dataset_two,
        left_on="International Security Identification Number",
        right_on="Company ISIN",
    )

    return merged_dataset


def get_data(file, isin_column, other_columns):
    """ Loads the data, and removes rows without valid ISIN. """

    if type(other_columns) != list:
        raise ValueError("other_columns should be a list")

    data = load_from_excel_file(file)

    # removes unnecessary columns
    data = pd.DataFrame(data, columns=[isin_column, *other_columns])
    print(f"Loaded columns from {file}: \n{list(data.columns)} \n")

    return data


def save_to_excel_file(data, filename="merged_data"):
    """
    Saves the data to an Excel-file with the given filename. The file is stored in the data-folder
    """

    print(f"Saving data to file: {filename}.xlsx\n")
    data.to_excel(f"data/{filename}.xlsx")
    print("Data saved")


def load_from_excel_file(filename):
    """ Loads the data from the file """

    print(f"Loading {filename}")
    return pd.read_excel(f"data/{filename}.xlsx")


if __name__ == "__main__":
    main()

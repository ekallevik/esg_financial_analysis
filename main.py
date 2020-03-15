import pandas as pd


def main():

    data_set_one = get_data_set(
        data_set="data1",
        isin_column="International Security Identification Number",
        other_columns=["Data Date - Daily Prices", "Shares Outstanding", "Price - Close - Daily"],
    )
    data_set_one = remove_irrelevant_rows(
        data_set_one, column="Data Date - Daily Prices", matching_value=20030102
    )
    save_to_excel_file(data_set_one, filename="data1_clean")

    data_set_two = get_data_set(
        data_set="data2", isin_column="Company ISIN", other_columns=["ESG 2003"]
    )
    save_to_excel_file(data_set_two, filename="data2_clean")

    merged_data_set = merge_data_sets(dataset_one=data_set_one, dataset_two=data_set_two)
    save_to_excel_file(merged_data_set, filename="merged_data")


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


def get_data_set(data_set, isin_column, other_columns):
    """
    Loads the data, and removes rows without valid ISIN.
    """

    if type(other_columns) != list:
        raise ValueError("other_columns should be a list")

    data = load_from_excel_file(data_set)

    dataframe = pd.DataFrame(data, columns=[isin_column, *other_columns])
    print(f"Loaded columns from {data_set}: \n{list(dataframe.columns)} \n")

    dataframe = remove_companies_without_valid_isin(dataframe, isin_column)

    return dataframe


def remove_irrelevant_rows(dataframe, column, matching_value):
    """
    Removes any row where the value of the cell (row, column) is not equal to matching_value.
    """

    old_length = len(dataframe[column])

    dataframe = dataframe.loc[dataframe[column] == matching_value]
    new_length = len(dataframe[column])
    percentage = 1.0 - new_length / old_length
    print(f"Removed irrelevant rows (reduction of {percentage:.2%})\n")

    return dataframe


def remove_companies_without_valid_isin(dataframe, isin_column):
    """
    Removes any row without a ISIN number.
    """

    old_length = len(dataframe[isin_column])

    dataframe.dropna(subset=[isin_column], inplace=True)
    new_length = len(dataframe[isin_column])
    percentage = new_length / old_length

    print(
        f"Removed {old_length - new_length} records without valid ISIN (reduction of {1.0 - percentage:.2%})"
    )

    return dataframe


def save_to_excel_file(dataframe, filename="merged_data"):
    """
    Saves the dataframe to an Excel-file with the given filename. The file is stored in the data-folder
    """

    print(f"Saving dataframe to file: {filename}.xlsx\n")
    dataframe.to_excel(f"data/{filename}.xlsx")
    print("Dataframe saved")


def load_from_excel_file(data_set):
    """
    Loads the data set from file
    """

    print(f"Loading {data_set}")
    return pd.read_excel(f"data/{data_set}.xlsx")


if __name__ == "__main__":
    main()

import csv
import hashlib
from datetime import timedelta, datetime

BASE_PATH = "./csv"

TYPE_CODES = [
    "COLO",
    "DELO",
    "LTL",
    "RELO",
    "STL",
    "SYNLO",
    "ISULO",
    "SULO",
    "TL",
    "COMORT",
    "COMORTCON",
    "CLMORT",
    "RESMORTCON",
    "RESMORTINS",
    "RESMORTINV",
    "RESMORTLO",
    "RESMORTSAV"
]


def generate_hub_and_sat_test_data(number_of_hub_inserts, number_of_sat_inserts_per_hub):
    """
    Generates the contents of the hub and satellite tables.

    Args:
        number_of_hub_inserts (int): number of rows in the hub
        number_of_sat_inserts_per_hub (int): number of rows in the satellite per hub entry
    """    
    hub_file_name = f"{BASE_PATH}/hub_{number_of_hub_inserts}.csv"
    with open(hub_file_name, "w+") as f:
        print(f"Generating file: {hub_file_name}")
        writer = csv.writer(f)
        start_date = datetime(2020,1,1,2,0)
        sat_inserts = []

        for i in range(number_of_hub_inserts):
            primary_key = hashlib.md5(f"primary_key_{i}".encode()).hexdigest()

            writer.writerow([
                primary_key,
                f"my_unique_id_{i}",
                "system of records",
                (start_date + timedelta(days=i))
            ])

            for j in range(number_of_sat_inserts_per_hub):
                sat_inserts.append([
                    primary_key,
                    start_date + timedelta(days=i, seconds=j),
                    "system of records",
                    hashlib.md5(f"{primary_key}-{j}".encode()).hexdigest(),
                    j % 100000,
                    j % 25,
                    (start_date + timedelta(days=i + (10*j))),
                    f"{TYPE_CODES[j % len(TYPE_CODES)]}{j % 20}"
                ])
        
        sat_file_name = f"{BASE_PATH}/sat_{number_of_hub_inserts * number_of_sat_inserts_per_hub}.csv"
        with open(sat_file_name, "w+") as g:
            print(f"Generating file: {sat_file_name}")
            sat_writer = csv.writer(g)
            sat_writer.writerows(sat_inserts)


def generate_reference_test_data(number_of_reference_inserts):
    """
    Generates the contents of the reference table.

    Args:
        number_of_reference_inserts (int): number of rows for your reference table, must be a multiple of 20.
    """    

    template = lambda x: f"""COLO{x}, COLLATERALIZED LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
DELO{x}, DEMAND LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
LTL{x}, LONG TERM LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
RELO{x}, REVOLVING LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
STL{x}, SHORT TERM LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
SYNLO{x}, SYNDICATED LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
CONVLO{x}, CONVERTIBLE LOAN TYPE {x}, LOAN AGREEMENT,2020-01-01 02:00:00,system of records
ISULO{x}, ISSUED SUBORDINATED LOAN TYPE {x}, SUBORDINATED LOANS,2020-01-01 02:00:00,system of records
SULO{x}, SUBORDINATED LOAN TYPE {x}, SUBORDINATED LOANS,2020-01-01 02:00:00,system of records
TL{x}, TRADE LOAN TYPE {x}, TRADE LOAN,2020-01-01 02:00:00,system of records
COMORT{x}, COMMERCIAL MORTGAGES TYPE {x}, MORTGAGES,2020-01-01 02:00:00,system of records
COMORTCON{x}, COMMERCIAL MORTGAGE CONSTRUCTION DEPOT TYPE {x}, MORTGAGES,2020-01-01 02:00:00,system of records
CLMORT{x}, CREDIT LINE MORTGAGE TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTCON{x}, RESIDENTIAL MORTGAGE CONSTRUCTION DEPOT TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTINS{x}, RESIDENTIAL MORTGAGE INSURANCE POLICY TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTBEACH{x}, RESIDENTIAL MORTGAGE BEACH HOUSE TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTINV{x}, RESIDENTIAL MORTGAGE INVESTMENT ACCOUNT TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTLO{x}, RESIDENTIAL MORTGAGE LOAN, RESIDENTIAL MORTGAGES TYPE {x},2020-01-01 02:00:00,system of records
RESMORTSAV{x}, RESIDENTIAL MORTGAGE SAVINGS ACCOUNT TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records
RESMORTSPEND{x}, RESIDENTIAL MORTGAGE SPENDING ACCOUNT TYPE {x}, RESIDENTIAL MORTGAGES,2020-01-01 02:00:00,system of records"""

    if number_of_reference_inserts % 20 != 0:
        raise ValueError("Number of reference inserts should be multiple of 20.")

    number_of_templates = int(number_of_reference_inserts / 20)
    ref_file_name = f"{BASE_PATH}/reference_{number_of_reference_inserts}.csv"
    with open(ref_file_name, "w+") as f:
        print(f"Generating file: {ref_file_name}")
        f.write(
            "\n".join([template(i) for i in range(number_of_templates)])
        )


def main():
    # The number of hubs is always 10000
    number_of_hub_inserts = 10000

    # We want 10.000, 100.000, 1.000.000, 5.000.000, 25.000.000 inserts into Sat and Reference
    for multiplier in [1, 10, 100, 500, 2500]:
        generate_hub_and_sat_test_data(number_of_hub_inserts, multiplier)
        generate_reference_test_data(number_of_hub_inserts * multiplier)

if __name__ == "__main__":
    main()
    

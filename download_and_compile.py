import os
import zipfile
import pandas as pd
import requests
import configparser
from tqdm import tqdm
from datetime import datetime


# Yields all year-month combinations from now now to start
def get_year_months(start_year, start_month):
    now = datetime.now()
    year, month = now.year, now.month
    while year > start_year or (year == start_year and month > start_month):
        yield year, month

        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1


# Function to extract year and month from URL
def extract_period(url):
    filename = url.split("=")[-1].replace(".ZIP", "")
    period = filename.split("_")[-1]
    year = period[:4]
    month = period[4:]
    return year, month


# Function to download a file with progress bar
def download_file_with_progress(url, zip_path):
    response = requests.get(
        url,
        stream=True,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"
        },
    )
    first_chunk = next(response.iter_content(1024), None)

    if not first_chunk:
        print("Error: Empty response")
        return False

    if b"<html" in first_chunk.lower() or first_chunk.startswith(b"<!DOCTYPE"):
        print("Error: Remote ZIP does not exist (HTML page returned)")
        return False

    if not first_chunk.startswith(b"PK"):
        print("Error: Response is not a valid ZIP file")
        return False

    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024

    progress_bar = tqdm(
        total=total_size,
        unit="iB",
        unit_scale=True,
        desc="Downloading",
    )

    with open(zip_path, "wb") as f:
        f.write(first_chunk)
        progress_bar.update(len(first_chunk))

        for chunk in response.iter_content(block_size):
            if chunk:
                f.write(chunk)
                progress_bar.update(len(chunk))

    progress_bar.close()
    return True


# Function to process and append each dataframe to CSV
def process_and_append_to_csv(
    year, month, prefix, url_template, download_dir, output_file
):
    period = f"{year}{month:02d}"
    url = url_template.format(period=period)
    zip_path = os.path.join(download_dir, f"BASE_DE_DADOS_CNES_{period}.ZIP")

    if not os.path.exists(zip_path):
        success = download_file_with_progress(url, zip_path)
        if not success:
            print(f"Error downloading file: {url}")
            return False
    else:
        print("Reading from cached file:", zip_path)

    success = True
    try:
        with zipfile.ZipFile(zip_path) as z:
            tipo_unidade_file = f"tbTipoUnidade{period}.csv"
            estabelecimento_file = f"tbEstabelecimento{period}.csv"
            tipo_estabelecimento_file = f"tbTipoEstabelecimento{period}.csv"

            with (
                z.open(tipo_unidade_file) as tipo_unidade_csv,
                z.open(estabelecimento_file) as estabelecimento_csv,
            ):
                tipo_unidade_df = pd.read_csv(
                    tipo_unidade_csv,
                    sep=";",
                    encoding="latin1",
                    usecols=["CO_TIPO_UNIDADE", "DS_TIPO_UNIDADE"],
                    dtype={"CO_TIPO_UNIDADE": "Int64", "DS_TIPO_UNIDADE": "str"},
                )

                estabeleciment_dtypes = {
                    "CO_UNIDADE": "str",
                    "CO_CNES": "Int64",
                    "NO_RAZAO_SOCIAL": "str",
                    "NO_FANTASIA": "str",
                    "NO_LOGRADOURO": "str",
                    "NU_ENDERECO": "str",
                    "NO_COMPLEMENTO": "str",
                    "NO_BAIRRO": "str",
                    "TP_UNIDADE": "Int64",
                    "CO_MUNICIPIO_GESTOR": "Int64",
                    "CO_TIPO_ESTABELECIMENTO": "Int64",
                }
                estabelecimento_df = pd.read_csv(
                    estabelecimento_csv,
                    sep=";",
                    encoding="latin1",
                    usecols=lambda col: col in estabeleciment_dtypes.keys(),
                    dtype=estabeleciment_dtypes,
                )
                estabelecimento_df = estabelecimento_df[
                    estabelecimento_df["CO_UNIDADE"].str.startswith(prefix)
                ]

                try:
                    with z.open(tipo_estabelecimento_file) as tipo_estabelecimento_csv:
                        tipo_estabelecimento_df = pd.read_csv(
                            tipo_estabelecimento_csv,
                            sep=";",
                            encoding="latin1",
                            usecols=[
                                "CO_TIPO_ESTABELECIMENTO",
                                "DS_TIPO_ESTABELECIMENTO",
                            ],
                            dtype={
                                "CO_TIPO_ESTABELECIMENTO": "Int64",
                                "DS_TIPO_ESTABELECIMENTO": "str",
                            },
                        )

                        merged_df = pd.merge(
                            estabelecimento_df,
                            tipo_estabelecimento_df,
                            left_on="CO_TIPO_ESTABELECIMENTO",
                            right_on="CO_TIPO_ESTABELECIMENTO",
                            how="left",
                        )
                except KeyError:
                    print(
                        f"Warning: {tipo_estabelecimento_file} not found in {zip_path}. Proceeding without it."
                    )
                    merged_df = estabelecimento_df
                    merged_df["CO_TIPO_ESTABELECIMENTO"] = pd.NA
                    merged_df["DS_TIPO_ESTABELECIMENTO"] = ""
                    success = False

                merged_df = pd.merge(
                    merged_df,
                    tipo_unidade_df,
                    left_on="TP_UNIDADE",
                    right_on="CO_TIPO_UNIDADE",
                    how="left",
                )

                merged_df["year"] = year
                merged_df["month"] = month

                merged_df.to_csv(
                    output_file,
                    mode="a",
                    index=False,
                    header=not os.path.exists(output_file),
                )

    except zipfile.BadZipFile:
        print(f"Corrupted zip file: {zip_path}. Skipping...")
        success = False
    except OSError as e:
        print(f"OSError processing {zip_path}: {e}. Skipping...")
        success = False

    return success


# Load configurations for download and compilation
config = configparser.ConfigParser()
config.read("config.ini")

# Prepare folders and files
download_dir = config.get("download", "destination_folder")
os.makedirs(download_dir, exist_ok=True)

prefix = config.get("compile", "co_unidade_prefix")
output_file = os.path.join(".", f"cnes_estab_full_{prefix}.csv")
if os.path.exists(output_file):
    print(f"Output file {output_file} already exists. It will be re-created.")
    os.remove(output_file)

# Check if only last period should be considered
only_last_period = config.getboolean("compile", "compile_only_last_period")

# Iterate over each period, downloading files and processing them
start_year = config.getint("download", "start_year")
start_month = config.getint("download", "start_month")
url_template = config.get("download", "url_template")
for year, month in get_year_months(start_year, start_month):
    result = process_and_append_to_csv(
        year, month, prefix, url_template, download_dir, output_file
    )

    if result is True and only_last_period:
        break

print("Processing complete. Data saved to ", output_file)

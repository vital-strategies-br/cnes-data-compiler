# CNES Data Downloader & Compiler

## English

This repository provides scripts to download and compile establishment data from the Brazilian CNES (Cadastro Nacional de Estabelecimentos de Saúde) database.  
It downloads monthly ZIP files, extracts relevant CSVs, filters by a specified code prefix (e.g., state or municipality), and merges the data into a single CSV.

### Usage

1. **Configure**: Edit [`config.ini`](config.ini) to set the download folder, start year/month, URL template, and the prefix for filtering establishments.
2. **Run**:  
   ```sh
   python download_and_compile.py
   ```
3. The merged CSV will be saved as `cnes_estab_full_<prefix>.csv`.

### Requirements

- Python 3
- Packages: `pandas`, `requests`, `tqdm`

---

## Português

Este repositório contém scripts para baixar e compilar dados de estabelecimentos do CNES (Cadastro Nacional de Estabelecimentos de Saúde) do Brasil.  
Os scripts baixam arquivos ZIP mensais, extraem os CSVs relevantes, filtram pelo prefixo de código especificado (ex: estado ou município) e unem os dados em um único CSV.

### Uso

1. **Configurar**: Edite o arquivo [`config.ini`](config.ini) para definir a pasta de download, ano/mês inicial, template da URL e o prefixo para filtrar estabelecimentos.
2. **Executar**:  
   ```sh
   python download_and_compile.py
   ```
3. O CSV final será salvo como `cnes_estab_full_<prefix>.csv`.

### Requisitos

- Python 3
- Pacotes: `pandas`, `requests`, `tqdm`

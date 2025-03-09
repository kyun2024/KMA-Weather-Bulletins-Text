# KMA Weather Bulletins Text

This repository contains the weather bulletins released by the Korea Meteorological Administration (KMA) for the years 2017 to 2019. The bulletins are parsed from PDF files into text format for easier processing and analysis.

The original bulletins are available through the [public data portal (data.go.kr)](https://www.data.go.kr/).

## Directory Structure
- **pdf**: Contains the original weather bulletins in PDF format. The directory structure follows a **year/month** hierarchy (e.g., `2017/01/` for January 2017 bulletins).
- **txt**: Contains the parsed text files. The directory structure mirrors that of the `pdf` folder, with `.txt` files corresponding to each PDF.
- **src**: Contains the scripts used to parse the PDF files into text. The main script is `pdf2text.py`.

## File Details

- **weather_bulletins_dataset_2017-2019.pkl**: A pickle file that contains a list of tuples. Each tuple includes the *data issuance time* and the corresponding *weather bulletin text*. This file is used for further analysis and research.

## Usage of `pdf2text.py`

To parse the weather bulletins, you can use the `pdf2text.py` script in the `src` directory. The script uses **tabula-py** (with jpype for faster processing).

### Running the Script

```bash
python src/pdf2text.py <pdf_dir> <txt_dir> [--dry-run] [--num-processes <num>]
```

### Arguments:
- `pdf_dir` (str): The directory containing the PDF files to be processed. It should contain subdirectories organized by year and month.
- `txt_dir` (str): The directory where the parsed text files will be saved. The directory structure will mirror that of the `pdf_dir`, with `.txt` files replacing the `.pdf` extensions.
- `--dry-run` (optional): If specified, the script will only print the paths of the PDF and corresponding text files without performing any actual parsing or file writing.
- `--num-processes` (optional): Specifies the number of processes to be used for parallel processing. Default is 4.

### Example Usage:
```bash
python src/pdf2text.py --dry-run ../pdf ../txt
python src/pdf2text.py ../pdf ../txt --num-processes 8

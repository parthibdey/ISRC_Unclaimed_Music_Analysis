# ISRC Unclaimed Music Rights Analysis

This project analyzes unclaimed music rights by cross-referencing artist catalogs from Spotify with a large dataset of unclaimed musical works. The analysis helps identify tracks that may have unclaimed royalties or rights issues.

## Features

- Process large TSV files (millions of records) into a local SQLite database for efficient querying.
- Retrieve full artist catalogs from Spotify, including all albums, singles, and compilations.
- Match tracks with unclaimed works using ISRC codes.
- Generate professional Excel reports with:
  - Artist Catalog
  - Unclaimed Matches
  - Analysis Summary

## Project Structure

ISRC_Unclaimed_Music_Analysis/
│
├── analysis.py # Main script for analysis
├── .env # Environment variables (ignored by Git)
├── requirements.txt # Project dependencies
├── README.md # This file
├── output/ # Folder for Excel reports
└── unclaimed_works.db # SQLite database of unclaimed works

## Setup Instructions

1. **Clone the repository**:

```bash
git clone https://github.com/yourusername/ISRC_Unclaimed_Music_Analysis.git
cd ISRC_Unclaimed_Music_Analysis 
```

2. **Create a virtual environment and activate it:**

```bash
python -m venv .venv
.\.venv\Scripts\activate  # Windows 
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Create a .env file** 

Add a .env file in the project root with the following keys:

```bash
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
TSV_FILE=path_to_unclaimedmusicalworkrightshares.tsv
DB_FILE=unclaimed_works.db
OUTPUT_DIR=output
```


## Usage


NOTE: After running once, you can comment out the database creation line in main() to avoid rebuilding the large database every time.

```bash
python assignment.py
```

This will:

1) Fetch the artist catalog from Spotify.

2) Match tracks with unclaimed works.

3) Generate an Excel report in the OUTPUT_DIR folder.

## Excel Report

The report includes:

- **Artist Catalog** – all tracks retrieved from Spotify.
- **Unclaimed Matches** – tracks by the artist that exist in the unclaimed works database.
- **Analysis Summary** – metrics like total tracks, matches found, match rate, and database info.
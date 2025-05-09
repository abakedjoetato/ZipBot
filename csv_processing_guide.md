# CSV Processing Guide

This guide provides an overview of the enhanced CSV processing capabilities in the Discord bot.

## Features

The CSV processor has been upgraded with the following features:

- **Smart Delimiter Detection**: Automatically detects and handles both comma and semicolon delimiters
- **Flexible Timestamp Parsing**: Supports over 20 different date/time formats
- **Empty File Handling**: Gracefully processes empty or malformed files
- **Intelligent Row Validation**: Extracts data from incomplete rows when possible
- **Enhanced Pattern Matching**: Better recognition of CSV files with different naming conventions

## Using the CSV Processor

### Basic Usage

```python
from utils.csv_parser import CSVParser

# Initialize the parser
parser = CSVParser(format_name="deadside")

# Process a CSV file
events = parser.process_csv_file("path/to/your/file.csv")

# Process CSV content directly
events = parser.process_csv_content(csv_content, file_path="optional_reference_name.csv")
```

### Options

```python
# Process only new lines since the last read
events = parser.process_csv_file("path/to/your/file.csv", only_new_lines=True)

# Specify the format name (if different from the default)
parser = CSVParser(format_name="custom_format")

# Override the default timestamp format
parser = CSVParser(format_name="deadside", datetime_format="%Y-%m-%d %H:%M:%S")
```

### Filtering Events

```python
from datetime import datetime, timedelta

# Initialize the parser
parser = CSVParser(format_name="deadside")

# Get all events
all_events = parser.process_csv_file("path/to/your/file.csv")

# Filter events by time range (last 24 hours)
start_time = datetime.now() - timedelta(days=1)
recent_events = parser.filter_events(all_events, start_time=start_time)

# Filter events by player
player_events = parser.filter_events(all_events, player_id="12345")

# Filter events by weapon
sniper_events = parser.filter_events(all_events, weapon="SniperRifle")

# Filter events by distance
long_shots = parser.filter_events(all_events, min_distance=100)
```

### Aggregating Statistics

```python
# Initialize the parser
parser = CSVParser(format_name="deadside")

# Get all events
all_events = parser.process_csv_file("path/to/your/file.csv")

# Aggregate player statistics
player_stats = parser.aggregate_player_stats(all_events)

# Aggregate events by time
hourly_stats = parser.aggregate_events_by_time(all_events, interval="hour")
```

## CSV File Format

The enhanced parser now supports various CSV formats, including:

### Standard Format (Comma-Separated)
```
2025.05.09-11.58.37,TestKiller,12345,TestVictim,67890,AK47,100,PC
```

### European Format (Semicolon-Separated)
```
2025.05.09-11.58.37;TestKiller;12345;TestVictim;67890;AK47;100;PC
```

### Different Timestamp Formats
All of these formats are now supported:
```
2025.05.09-11.58.37,Player,ID,Victim,ID,Weapon,Distance,Platform
2025-05-09 11:58:37,Player,ID,Victim,ID,Weapon,Distance,Platform
2025/05/09 11:58:37,Player,ID,Victim,ID,Weapon,Distance,Platform
09.05.2025-11.58.37,Player,ID,Victim,ID,Weapon,Distance,Platform
20250509-115837,Player,ID,Victim,ID,Weapon,Distance,Platform
```

## Troubleshooting

If you encounter issues with CSV processing:

1. **Check the file format**: Ensure your CSV files have the expected column structure
2. **Verify file access**: Make sure the bot has permission to read the CSV files
3. **Check for corrupt files**: Try opening the file in a text editor to verify it's not corrupted
4. **Enable debug logging**: Set the logging level to DEBUG for more detailed information
5. **Check timestamp format**: If timestamp parsing fails, ensure the format matches one of the supported formats

## Advanced Configuration

For specialized use cases, you can configure the CSV parser with:

```python
# Custom column names
parser = CSVParser(
    format_name="custom",
    columns=["time", "player1", "player1_id", "player2", "player2_id", "action", "value"]
)

# Custom datetime column and format
parser = CSVParser(
    format_name="custom",
    datetime_column="time",
    datetime_format="%Y-%m-%d %H:%M:%S"
)
```

## Testing the CSV Processor

Use the provided test script to verify the CSV processor is working correctly:

```bash
python test_csv_processing.py
```

This will process all CSV files in the specified directory and report the results.
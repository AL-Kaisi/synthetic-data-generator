# Custom Schemas Directory

This directory allows you to add your own JSON schema files that will be automatically discovered and made available in the synthetic data generator.

## How it Works

1. **Add Schema File**: Drop any valid JSON schema file (`.json`) into this directory
2. **Auto-Discovery**: The app automatically scans this directory and loads all schema files
3. **Use Schema**: The schema becomes available using the filename (without `.json` extension)

## Example

If you add a file called `my_custom_schema.json`, you can use it with:

```bash
python3 cli.py generate my_custom_schema -n 100
```

## Schema File Format

Your schema files must follow JSON Schema specification:

```json
{
  "type": "object",
  "title": "MySchema",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string", "maxLength": 50},
    "age": {"type": "integer", "minimum": 0, "maximum": 120}
  },
  "required": ["id", "name"]
}
```

## Supported Constraints

- **String**: `maxLength`, `pattern`, `enum`
- **Number/Integer**: `minimum`, `maximum`, `decimalPlaces`
- **Date strings**: `start`, `end` (e.g., "2020-01-01", "2024-12-31")
- **Arrays**: `items`, `minItems`, `maxItems`
- **Objects**: Nested properties
- **Required fields**: `required` array

## Examples Included

- `customer.json` - Customer records with subscription tiers
- `blog_post.json` - Blog posts with engagement metrics

## Tips

1. **Filename = Schema Name**: The filename becomes the schema identifier
2. **Validation**: Invalid schema files are automatically skipped with warnings
3. **Precedence**: Predefined schemas take precedence over custom ones
4. **Multiple Directories**: Files in both `custom_schemas/` and `schemas/` are discovered

## Usage Examples

```bash
# List all schemas (including custom ones)
python3 cli.py list

# Generate from custom schema
python3 cli.py generate customer -n 500 --format csv

# Generate massive dataset with PySpark
python3 cli.py generate blog_post -n 1000000 --spark --format parquet

# Use with different output formats
python3 cli.py generate my_schema -n 100 --format jsonld
```

Simply add your `.json` schema files here and they'll be automatically available!
# Data Quality Validation Framework

A comprehensive data quality validation framework built using Great Expectations that performs various validation checks on different data sources and generates detailed, beautiful reports.

## Features

### Validation Types
1. Count Validation
2. Checksum Validation
3. Business Rule Validation
4. Reconciliation Logic
5. Duplicate Check
6. Pattern Check
7. Enumeration Check
8. Mandatory Check
9. Range Check
10. Type Check
11. Unique Check

### Supported Data Sources
- SQL Server Tables
- Feed Files
- Flat Files
- Parquet Files
- AWS S3 Bucket Feed Files

### Report Generation
- Interactive HTML Reports
- PDF Reports
- Excel Reports with Multiple Sheets
- Email Distribution Capability

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-quality-framework
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The framework uses a YAML configuration file (`config/rules_dictionary.yaml`) that defines:

1. **Validations**: List of unique rules defining which type of rules need to be run
   - Rule_ID
   - Template Name
   - Columns Name
   - RuleType (Mandatory, Range, Type, Unique, etc.)

2. **Template**: Description of templates with columns:
   - Feed Name
   - Column Name
   - Definition
   - Enumeration
   - Type_Name
   - Range (Bottom, Top, Scope)
   - Mandatory
   - Unique

3. **Enumerations**: List of expected values for non-numeric fields

4. **Pattern**: Pattern expectations for specific feeds

5. **Business Rules**: Custom business validations

6. **Reconciliations**: Source/Target reconciliation rules

## Usage

### Basic Usage

```python
from dq_framework.main import DataQualityFramework

# Initialize the framework
dq_framework = DataQualityFramework()

# Load your data (example with pandas DataFrame)
data = pd.read_csv('your_data.csv')

# Run validations
validation_results = dq_framework.validate_data(data)

# Generate report
dq_framework.generate_report()
```

### Example Script

An example script (`example_usage.py`) is provided that demonstrates the framework's usage with sample data:

```bash
python example_usage.py
```

## Report Types

### 1. HTML Report
- Interactive visualizations
- Detailed validation results
- Expandable error samples
- Modern, responsive design
- Filtering and sorting capabilities

### 2. PDF Report
- Professional formatting
- Printer-friendly
- Ideal for sharing and archiving

### 3. Excel Report
- Multiple worksheets for different aspects
- Detailed error logs
- Pivot-table ready format

## Report Sections

1. **Executive Summary**
   - Overall validation status
   - Key metrics
   - Success rate

2. **Validation Details**
   - Results for each validation type
   - Error samples
   - Detailed statistics

3. **Visualizations**
   - Status distribution charts
   - Error trend analysis
   - Type-wise validation results

## Customization

### Adding New Validation Types

1. Add the validation configuration in `rules_dictionary.yaml`
2. Implement the validation logic in `dq_framework/validations.py`
3. Update the report template to display the new validation results

### Customizing Reports

1. Modify the HTML template in `dq_framework/reporting/templates/report_template.html`
2. Update the CSS styling
3. Add new visualizations in `report_generator.py`

## Best Practices

1. **Configuration Management**
   - Keep sensitive information in environment variables
   - Use separate configurations for different environments

2. **Data Source Handling**
   - Implement proper error handling for data source connections
   - Use connection pooling for database connections

3. **Report Distribution**
   - Set up automated report distribution for critical validations
   - Implement proper access controls for reports

## Error Handling

The framework implements comprehensive error handling:
- Detailed error logging
- Error samples in reports
- Graceful failure handling
- Error notifications

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

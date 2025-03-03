# Salesforce Data Cloud Integrator

An open-source connector for integrating Salesforce Data Cloud with third-party analytics platforms, improving data visualization and insights.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

The Salesforce Data Cloud Integrator is a comprehensive solution for enabling seamless data flow between Salesforce Data Cloud and various third-party analytics platforms such as Tableau, Power BI, and others. This connector facilitates data extraction, transformation, and loading, allowing organizations to leverage their Salesforce data for enhanced analytics and visualization.

## Features

- **Bi-directional Data Flow**: Extract data from Salesforce Data Cloud and push data back from external systems
- **Support for Multiple Analytics Platforms**: Pre-built connectors for Tableau, Power BI, and more
- **Scheduled Data Synchronization**: Set up automated data sync jobs with customizable schedules
- **Data Transformation**: Apply transformations to data during the integration process
- **Apex & Python Integration**: Use both Apex (for Salesforce-side operations) and Python (for external operations)
- **REST API**: Expose Data Cloud information via REST endpoints for third-party consumption
- **Error Handling & Logging**: Comprehensive error handling and logging mechanisms

## Architecture

The connector consists of two main components:

1. **Apex Classes**: Running within Salesforce to provide direct access to Data Cloud
2. **Python Connector**: External component for interacting with both Salesforce and analytics platforms

![Architecture Diagram](architecture_diagram.png)

## Installation

### Prerequisites

- Salesforce org with Data Cloud enabled
- Python 3.8+
- Tableau/Power BI account (for respective integrations)

### Salesforce Setup

1. Deploy the Apex classes to your Salesforce org:

```bash
sfdx force:source:deploy -p force-app/main/default/classes
```

2. Configure necessary permissions in Salesforce:

- Assign the "Data Cloud API Access" permission set to integration users
- Enable API access for the integration user

### Python Connector Setup

1. Clone this repository:

```bash
git clone https://github.com/your-username/salesforce-data-cloud-integrator.git
cd salesforce-data-cloud-integrator
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure the connector:

```bash
cp config.example.json config.json
# Edit config.json with your connection details
```

## Usage

### Basic Usage

```python
from salesforce_data_cloud_integrator import SalesforceDataCloudConnector

# Initialize connector
connector = SalesforceDataCloudConnector(config_path="config.json")

# Connect to Salesforce
connector.connect_to_salesforce()

# Query Data Cloud
query = """
SELECT Account.Name, Account.Industry, SUM(Amount) as TotalAmount
FROM Opportunity
WHERE CloseDate = THIS_YEAR
GROUP BY Account.Name, Account.Industry
"""
results = connector.query_data_cloud(query)

# Export to Tableau
connector.connect_to_tableau()
connector.upload_to_tableau("Annual Opportunities", "data.csv")
```

### Creating Data Pipelines

```python
# Create an end-to-end pipeline
result = connector.create_data_pipeline(
    query="SELECT Id, Name, AnnualRevenue FROM Account WHERE AnnualRevenue > 1000000",
    data_source_name="High Value Accounts",
    target="tableau"
)
```

### Using the Apex Controller

```apex
// Query Data Cloud from Apex
String jsonResults = DataCloudIntegrationController.getDataCloudQueryResults(
    'SELECT Id, Name FROM Account LIMIT 10'
);

// Create a scheduled sync job
String jobId = DataCloudIntegrationController.createDataSyncJob(
    'Daily Account Sync',
    'SELECT Id, Name, Industry FROM Account',
    '0 0 * * * ?',  // Daily at midnight
    '{"type": "tableau", "project": "Salesforce Data"}'
);
```

## API Reference

### Python API

#### `SalesforceDataCloudConnector`
The main connector class for interacting with Salesforce Data Cloud.

- `__init__(config_path=None, config=None)`: Initialize with either a config file path or dict
- `connect_to_salesforce()`: Establish connection to Salesforce
- `connect_to_tableau()`: Establish connection to Tableau Server
- `query_data_cloud(query)`: Execute SOQL query against Data Cloud
- `export_to_csv(data, filepath)`: Export DataFrame to CSV
- `upload_to_tableau(data_source_name, filepath)`: Upload to Tableau Server
- `create_data_pipeline(query, data_source_name, target)`: Create complete data pipeline

#### `ApexHandler`
Handler for Apex code execution and integration.

- `deploy_apex_class(class_path)`: Deploy an Apex class
- `execute_anonymous_apex(apex_code)`: Execute anonymous Apex code

#### `DataCloudETLJob`
ETL job manager for data integration.

- `run()`: Run the ETL job based on configuration

### Apex API

#### `DataCloudIntegrationController`
The main Apex controller for Data Cloud integration.

- `getDataCloudQueryResults(soql)`: Get query results from Data Cloud
- `createDataCloudDataset(datasetName, dataJson, schema)`: Create a Data Cloud dataset
- `getDataCloudObjectsMetadata()`: Get metadata about Data Cloud objects
- `createDataSyncJob(jobName, query, schedule, targetSystem)`: Create scheduled sync job
- `executeDataSyncJob(syncJobId)`: Execute a sync job immediately

## Configuration

### Sample Configuration

```json
{
  "salesforce": {
    "username": "your-username@example.com",
    "password": "your-password",
    "security_token": "your-security-token",
    "domain": "login"
  },
  "tableau": {
    "server_url": "https://your-tableau-server",
    "token_name": "your-token-name",
    "token_value": "your-token-value",
    "site_id": "your-site-id",
    "project_id": "your-project-id"
  }
}
```

## Examples

### Example 1: Exporting Opportunity Data to Tableau

```python
# Initialize connector
connector = SalesforceDataCloudConnector(config_path="config.json")

# Create pipeline
result = connector.create_data_pipeline(
    query="""
    SELECT Account.Name, SUM(Amount) as TotalAmount,
           AVG(Amount) as AverageAmount,
           MAX(Amount) as LargestDeal,
           COUNT(Id) as DealCount
    FROM Opportunity
    WHERE IsClosed = true AND IsWon = true
    GROUP BY Account.Name
    ORDER BY TotalAmount DESC
    LIMIT 50
    """,
    data_source_name="Top 50 Accounts by Revenue",
    target="tableau"
)
```

### Example 2: Scheduling Regular Data Sync

```apex
// Create daily sync job for account data
String jobId = DataCloudIntegrationController.createDataSyncJob(
    'Daily Account Data Sync',
    'SELECT Id, Name, Industry, AnnualRevenue, BillingCity, BillingState, BillingCountry FROM Account WHERE IsActive = true',
    '0 0 0 * * ?',  // Every day at midnight
    '{"type": "tableau", "project": "Sales Analytics", "datasource": "Account Data"}'
);

// Create weekly sync job for opportunity forecasting
String weeklyJobId = DataCloudIntegrationController.createDataSyncJob(
    'Weekly Opportunity Forecast',
    'SELECT Owner.Name, SUM(Amount) as ProjectedRevenue, CALENDAR_MONTH(CloseDate) as Month FROM Opportunity WHERE IsClosed = false GROUP BY Owner.Name, CALENDAR_MONTH(CloseDate)',
    '0 0 0 ? * SUN',  // Every Sunday at midnight
    '{"type": "powerbi", "workspace": "Sales", "dataset": "Opportunity Forecasts"}'
);
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Salesforce for their Data Cloud platform
- The open-source community for various dependencies used in this project

## Support

For support, please open an issue on GitHub or contact the maintainer directly.

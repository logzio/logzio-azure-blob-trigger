# Ship logs to Logz.io using Azure Blob Trigger

Auto-deployment of Azure resources for shipping logs from Azure storage using blob trigger. 
Each new log in the container path inside the storage account (including sub directories), will trigger the Logz.io function that will ship the file content to Logz.io.

## Getting Started

There are 2 options for auto-deployment. Choose the one that is suitable for you and follow the instructions:

### Full Auto-Deployment

If you don't have storage account (general purpose v2) with container with logs, or you want to create everything from scratch, this auto-deployment is for you. 
Will auto-deploy the following resources:

- Storage Account (general purpose v2) + Container
- App Service Plan - Consumption Plan
- Application Insights
- Logz.io Function App + Logz.io Blob Trigger Function
- Storage Account for Logz.io Function App logs

Press the button to start:

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Flogzio%2Flogzio-azure-blob-trigger%2Fmain%2Fazure%2Ffull-auto-deployment.json)

### Logz.io Function Auto-Deployment

If you already have storage account (general purpose v2) with container with logs, this auto-deployment is for you.
Will auto-deploy the following resources:

- App Service Plan - Consumption Plan
- Application Insights
- Logz.io Function App + Logz.io Blob Trigger Function
- Storage Account for Logz.io Function App logs

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Flogzio%2Flogzio-azure-blob-trigger%2Fmain%2Fazure%2Ffunction-auto-deployment.json)

* When deploying the Logz.io function, logs that were in the container before the deployment will be shipped to Logz.io.
If these logs have already been shipped to Logz.io, we recommend that you empty the container before the deployment or use the FilterDate and FilterDateJsonPath parameters.

## Instructions

You'll be taken to Azure Custom deployment page. Fill in all the parameters and click **Review + create** button:

![Screen_1](img/Screen_1.png)

| Parameter | Description | Is Required | Value If Empty |
| --- | --- | --- | --- |
| Storage Account Name | The storage account (general purpose v2) name. | Required | - |
| Storage Account Resource Name | The resource name that contains the storage account. (Needed only in Logz.io Function Auto-Deployment) | Required | - |
| Container Name | The name of the container inside the storage account | Required | - |
| Logs Path | The path from where blob files will trigger the Logz.io function (including subdirectories in that path). Leave empty if you want that every blob file in the container will trigger the Logz.io function. | Not Required | \<\<ContainerLogsPath\>\>/{name} |
| Format | The format of the log files. | Required | - |
| Logzio URL | The Logz.io listener URL fot your region. (For more details, see the regions page: https://docs.logz.io/user-guide/accounts/account-region.html) | Required | - |
| Logzio Token | Your Logz.io logs token. (Can be retrieved from the Manage Token page.) | Required | - |
| Multiline Regex | The regex that matches the multiline logs in text blob files. Leave empty if you do not use multiline logs in your text blob files. | Not Required | NO_REGEX |
| Datetime Filter | Every log with datetime greater or equal to this datetime will be shipped to Logz.io (for it to take effect DatetimeFinder and DatetimeFormat must not be empty). Leave empty if you want all logs to be shipped to Logz.io. | Not Required | NO_DATETIME_FILTER |
| Datetime Finder | If file is csv/json: write the json path of the datetime field inside each log. CSV json path will always be the name of the datetime field. Json json path can be the name of the datetime field if it's in the root, or a path contains fields separated by '.' (for example: metadata.datetime, metadata\[:1].datetime). If file is text: write a regex that will get the datetime from each log. If log has many occurrences of datetime, make sure the regex will give the right one (for example: '(?:.\*?\[0-9]){2}.*?(\[0-9])' will give the third digit). If this value cannot be found inside a log, the log will be shipped to Logz.io. Leave empty if you are not using DatetimeFilter. | Required if using DatetimeFilter | NO_DATETIME_FINDER |
| Datetime Format | The datetime format of DatetimeFilter and datetime field in each log (for example: %Y/%m/%dT%H:%M:%S%z is for 2021/11/01T10:10:10+0000 datetime). If the format is wrong, the log will be shipped to Logz.io. Leave empty if you are not using DatetimeFilter. | Required if using DatetimeFilter | NO_DATETIME_FORMAT |

On the following screen, press the **create** button:

![Screen_2](img/Screen_2.png)

If everything went well, you should see the following screen. Press **Go to resource group** button to go to your resource group with all the created resources:

![Screen_3](img/Screen_3.png)

## Resources

![Resources](img/Resources.png)

## Supported Data Types

The Logz.io function supports the following data types:

- Json
- CSV
- Text (supports multiline text - MultilineRegex parameter)

```
The file name **does not** have to be with these extensions.
```

## Supported File Formats

The Logz.io function supports the following file formats:

- Gzip

```
The file name **does not** have to be with these extensions.
```

## Searching in Logz.io

All logs that were sent from the function will be under the type `azure_blob_trigger` 

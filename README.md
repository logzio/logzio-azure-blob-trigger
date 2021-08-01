# Ship logs to Logz.io using Azure Blob Trigger

Auto-deployment of Azure resources for shipping logs from Azure storage using blob trigger. 
Each new log in the container path inside the storage account, will trigger the Logz.io function that will ship the file content to Logz.io.

## Getting Started

There are 2 options for auto-deployment. Choose the one that is suitable for you and follow the instructions:

### Full Auto-Deployment

If you don't have storage account with container with logs, or you want to create everything from scratch, this auto-deployment is for you. 
Will auto-deploy the following resources:

- Storage Account + Container
- App Service Plan - Consumption Plan
- Application Insights
- Logz.io Function App + Logz.io Blob Trigger Function

Press the button to start:

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Flogzio%2Flogzio-azure-blob-trigger%2Ffirst%2Fazure%2Ffull-auto-deployment.json)

### Logz.io Function Auto-Deployment

If you already have storage account with container with logs, this auto-deployment is for you.
Will auto-deploy the following resources:

- App Service Plan - Consumption Plan
- Application Insights
- Logz.io Function App + Logz.io Blob Trigger Function

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Flogzio%2Flogzio-azure-blob-trigger%2Ffirst%2Fazure%2Ffunction-auto-deployment.json)

## Instructions

You'll be taken to Azure Custom deployment page. Fill in all the parameters and click **Review + create** button:

![Screen_1](img/Screen_1.png)

| Parameter | Description |
| --- | --- |
| Storage Account Name | The storage account name. |
| Container Name | The name of the container inside the storage account |
| Logs Path | The path of the logs in the container. Leave empty if the logs are in root directory of the container. |
| Logzio URL | The Logz.io listener URL fot your region. (For more details, see the regions page: https://docs.logz.io/user-guide/accounts/account-region.html) |
| Logzio Token | Your Logz.io logs token. (Can be retrieved from the Manage Token page.)

On the following screen, press the **create** button:

![Screen_2](img/Screen_2.png)

If everything went well, you should see the following screen. Press **Go to resource group** button to go to your resource group with all the created resources:

![Screen_3](img/Screen_3.png)

## Supported Data Types

Every new log file inside the storage account's container, will trigger the Logz.io function, which will ship the logs to Logz.io.
This function supports the following data types:

- Json
- CSV
- Text

* The file name **does not** have to be with these extensions.

## Resources

![Resources](img/Resources.png)
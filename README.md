
# PDI Dropbox plugin
PDI plugin that offers Input and Ouput steps for Dropbox.

## Dropbox Input
The dropbox input step reads information from a Dropbox storage account.

### Fields
Field  | Description
------------- | -------------
Step name  | Specify the unique name of the Dropbox Input step on the canvas. You can customize the name or leave it as the default.
Access Token  | Dropbox access token. The access token is a string generated by Dropbox to uniquely identify both your app and the end user. Thus, subsequent API calls won't need to transmit the user's Dropbox password.
Source Files  |  Files to be read from Dropbox.
Target Files  |  Target local filesystem locations to write the files.

### Example
In this example, we are downloading a file called 'remoteFile.txt' to our local filesystem, as 'localFile.txt'.
![alt text](https://github.com/LeonardoCoelho71950/pdi-dropbox-plugin/blob/master/screenshots/dropbox-input.png "Downloading a file from Dropbox")

## Dropbox Ouput
The dropbox ouput step writes information to a Dropbox storage account.

### Fields
Field  | Description
------------- | -------------
Step name  | Specify the unique name of the Dropbox Output step on the canvas. You can customize the name or leave it as the default.
Access Token  | Dropbox access token. The access token is a string generated by Dropbox to uniquely identify both your app and the end user. Thus, subsequent API calls won't need to transmit the user's Dropbox password.
Source Files  |  Files to be read from the local file system.
Target Files  |  Target Dropbox remote locations to writes the files. 

### Example
In this example, we are uploading a file called 'localFile.txt' to our Dropbox storage, as 'remoteFile.txt'.
![alt text](https://github.com/LeonardoCoelho71950/pdi-dropbox-plugin/blob/master/screenshots/dropbox-output.png "Uploading a file to Dropbox")

# Authors:

- Leonardo Coelho	- <leonardo.coelho@ua.pt>

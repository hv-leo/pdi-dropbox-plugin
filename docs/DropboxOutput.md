# Dropbox Ouput
The dropbox ouput step writes information to a Dropbox storage account.

### Fields
Field  | Description
------------- | -------------
Send successful transfers to step  |  Successful transfers are sent to this step.
Send failed transfers to step  |  Failed transfers are sent to this step.
Step name  | Specify the unique name of the Dropbox Output step on the canvas.
Access Token  | Dropbox access token. Subsequent API calls won't need to transmit the user's Dropbox password.
Source Files  |  Files to be read from the local file system.
Target Files  |  Target Dropbox remote locations to writes the files. 

### Example
In this example: 
* A file, called ```localFile.txt```, was successfully uploaded to our Dropbox storage, as ```remoteFile.txt```.
* And there was a failed upload attempt of an inexistent file, called ```notFound.txt```.

![alt text](https://github.com/LeonardoCoelho71950/pdi-dropbox-plugin/blob/master/docs/screenshots/dropbox-output.png "Uploading a file to Dropbox")

# Dropbox Input
The dropbox input step reads information from a Dropbox storage account.

### Fields
Field  | Description
------------- | -------------
Send successful transfers to step  |  Successful transfers are sent to this step.
Send failed transfers to step  |  Failed transfers are sent to this step.
Step name  | Specify the unique name of the Dropbox Input step on the canvas.
Access Token  | Dropbox access token. Subsequent API calls won't need to transmit the user's Dropbox password.
Source Files  |  Files to be read from Dropbox.
Target Files  |  Target local filesystem locations to write the files.

### Example
In this example: 
* A file, called ```remoteFile.txt```, was successfully downloaded to our local filesystem, as ```localFile.txt```.
* And there was a failed download attempt of an inexistent file, called ```notFound.txt```.

![alt text](https://github.com/LeonardoCoelho71950/pdi-dropbox-plugin/blob/master/screenshots/dropbox-input.png "Downloading a file from Dropbox")

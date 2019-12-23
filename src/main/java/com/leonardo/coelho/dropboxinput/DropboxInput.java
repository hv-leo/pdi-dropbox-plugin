/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.leonardo.coelho.dropboxinput;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.google.common.io.Files;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Describe your step plugin.
 * 
 */
public class DropboxInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = DropboxInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private DropboxInputMeta meta;
  private DropboxInputData data;

  public DropboxInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param stepMetaInterface
     *          The metadata to work with
     * @param stepDataInterface
     *          The data to initialize
     */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    meta = (DropboxInputMeta) stepMetaInterface;
    data = (DropboxInputData) stepDataInterface;

    if ( super.init( stepMetaInterface, stepDataInterface ) ) {
      if ( Utils.isEmpty( meta.getAccessTokenField() ) ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Missing.AccessToken" ) );
        return false;
      }
      if ( Utils.isEmpty( meta.getSourceFilesField() ) ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Missing.SourceFiles" ) );
        return false;
      }
      if ( Utils.isEmpty( meta.getTargetFilesField() ) ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Missing.TargetFiles" ) );
        return false;
      }
      List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
      data.chosesTargetSteps =
        targetStreams.get( 0 ).getStepMeta() != null || targetStreams.get( 1 ).getStepMeta() != null;
      return true;
    } else {
      return false;
    }
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (DropboxInputMeta) smi;
    data = (DropboxInputData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      // Mapping Access Token field.
      data.accessTokenIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getAccessTokenField() );
      if ( data.accessTokenIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.AccessToken" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
      // Mapping Source Files field.
      data.sourceFileIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getSourceFilesField() );
      if ( data.sourceFileIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.SourceFiles" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
      // Mapping Target Files field.
      data.targetFilesIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getTargetFilesField() );
      if ( data.targetFilesIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.TargetFiles" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
      data.outputRowMeta = getInputRowMeta().clone();

      // Cache the position of the RowSet for the output.
      if ( data.chosesTargetSteps ) {
        List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
        if ( !Utils.isEmpty( targetStreams.get( 0 ).getStepname() ) ) {
          data.successfulRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 0 ).getStepname(), 0 );
          if ( data.successfulRowSet == null ) {
            throw new KettleException( BaseMessages.getString(
              PKG, "DropboxInput.Log.TargetStepInvalid", targetStreams.get( 0 ).getStepname() ) );
          }
        } else {
          data.successfulRowSet = null;
        }

        if ( !Utils.isEmpty( targetStreams.get( 1 ).getStepname() ) ) {
          data.failedRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 1 ).getStepname(), 0 );
          if ( data.failedRowSet == null ) {
            throw new KettleException( BaseMessages.getString(
              PKG, "DropboxInput.Log.TargetStepInvalid", targetStreams.get( 1 ).getStepname() ) );
          }
        } else {
          data.failedRowSet = null;
        }
      }
    }

    // Get Values from Input Row.
    String accessToken = (String) r[data.accessTokenIdx];
    String sourceFile = (String) r[data.sourceFileIdx];
    String targetFile = (String) r[data.targetFilesIdx ];

    if ( Utils.isEmpty( accessToken ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.AccessToken" ) );
      putFailedTransferRow( r );
      return true;
    }

    if ( Utils.isEmpty( sourceFile ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.SourceFiles" ) );
      putFailedTransferRow( r );
      return true;
    }

    if ( Utils.isEmpty( targetFile ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.TargetFiles" ) );
      putFailedTransferRow( r );
      return true;
    }

    // Create a DbxClientV2 to make API calls.
    DbxRequestConfig requestConfig = new DbxRequestConfig( "examples-download-file" );
    DbxClientV2 dbxClient = new DbxClientV2( requestConfig, accessToken );

    log.logBasic( BaseMessages.getString( PKG, "DropboxInput.log.Downloading", sourceFile ) );
    DbxDownloader<FileMetadata> downloader = null;
    try {
      downloader = dbxClient.files().download( sourceFile );
    } catch ( DbxException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.DownloadError", ex.getMessage() ) );
      putFailedTransferRow( r );
      return true;
    }
    try {
      // Create file and all non-existent parent folders.
      File localFile = new File( targetFile );
      Files.createParentDirs( localFile );
      FileOutputStream out = new FileOutputStream( localFile );
      downloader.download( out );
      out.close();
    } catch ( DbxException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.DownloadError", ex.getMessage() ) );
      putFailedTransferRow( r );
      return true;
    } catch ( FileNotFoundException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.FileNotFound", targetFile ) );
      putFailedTransferRow( r );
      return true;
    } catch ( IOException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.ErrorReadingFile", sourceFile, ex.getMessage() ) );
      putFailedTransferRow( r );
      return true;
    }
    log.logBasic( BaseMessages.getString( PKG, "DropboxInput.log.Downloaded", targetFile ) );

    putSuccessfulTransferRow( r ); // Transfer has succeeded.

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "DropboxInput.Log.LineNumber" ) + getLinesRead() );
    }
    return true;
  }

  private void putFailedTransferRow( Object[] r ) throws KettleStepException {
    if ( !data.chosesTargetSteps ) {
      putRow( data.outputRowMeta, r );
    } else {
      putRowTo( data.outputRowMeta, r, data.failedRowSet );
    }
  }

  private void putSuccessfulTransferRow( Object[] r ) throws KettleStepException {
    if ( !data.chosesTargetSteps ) {
      putRow( data.outputRowMeta, r );
    } else {
      putRowTo( data.outputRowMeta, r, data.successfulRowSet );
    }
  }
}

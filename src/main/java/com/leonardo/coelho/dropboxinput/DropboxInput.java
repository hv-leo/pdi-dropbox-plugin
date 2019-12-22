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
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Describe your step plugin.
 * 
 */
public class DropboxInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = DropboxInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private DropboxInputMeta meta;
  private DropboxInputData data;

  private int failedTransfers = 0;

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

      // Mapping SourceFiles field.
      if ( Utils.isEmpty( meta.getSourceFilesField() ) ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Missing.SourceFiles" ) );
        return false;
      }

      // Mapping TargetFiles field.
      if ( Utils.isEmpty( meta.getTargetFilesField() ) ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Missing.TargetFiles" ) );
        return false;
      }

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

    // We need to map the fields.
    if ( first ) {
      first = false;

      data.accessTokenIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getAccessTokenField() );
      if ( data.accessTokenIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.AccessToken" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }

      data.sourceFileIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getSourceFilesField() );
      if ( data.sourceFileIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.SourceFiles" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }

      data.targetFilesIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getTargetFilesField() );
      if ( data.targetFilesIdx < 0 ) {
        logError( BaseMessages.getString( PKG, "DropboxInput.Invalid.TargetFiles" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
    }

    // Get Values from Input Row.
    String accessToken = (String) r[data.accessTokenIdx];
    String sourceFile = (String) r[data.sourceFileIdx];
    String targetFile = (String) r[data.targetFilesIdx ];

    if ( Utils.isEmpty( accessToken ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.AccessToken" ) );
      setErrors( ++failedTransfers );
      return true;
    }

    if ( Utils.isEmpty( sourceFile ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.SourceFiles" ) );
      setErrors( ++failedTransfers );
      return true;
    }

    if ( Utils.isEmpty( targetFile ) ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Null.TargetFiles" ) );
      setErrors( ++failedTransfers );
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
      setErrors( ++failedTransfers );
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
      setErrors( ++failedTransfers );
      return true;
    } catch ( FileNotFoundException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.FileNotFound", targetFile ) );
      setErrors( ++failedTransfers );
      return true;
    } catch ( IOException ex ) {
      logError( BaseMessages.getString( PKG, "DropboxInput.Log.ErrorReadingFile", sourceFile, ex.getMessage() ) );
      setErrors( ++failedTransfers );
      return true;
    }
    log.logBasic( BaseMessages.getString( PKG, "DropboxInput.log.Downloaded", targetFile ) );

    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "DropboxInput.Log.LineNumber" ) + getLinesRead() );
    }
    return true;
  }
}

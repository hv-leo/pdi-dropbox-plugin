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
package com.leonardo.coelho.dropboxoutput;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepIOMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Objects;


/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "DropboxOutput", image = "DropboxOutput.svg", name = "Dropbox Output",
    description = "Writes to Dropbox.", categoryDescription = "Output" )
public class DropboxOutputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DropboxOutput.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private String accessTokenField;
  private String sourceFilesField;
  private String targetFilesField;

  public DropboxOutputMeta() {
    super(); // allocate BaseStepMeta
  }

  public void setSuccessfulStepname( String successfulStepname ) {
    getStepIOMeta().getTargetStreams().get( 0 ).setSubject( successfulStepname );
  }

  public String getSuccessfulStepname() {
    return getTargetStepName( 0 );
  }

  public void setFailedStepname( String failedStepname ) {
    getStepIOMeta().getTargetStreams().get( 1 ).setSubject( failedStepname );
  }

  public String getFailedStepname() {
    return getTargetStepName( 1 );
  }

  private String getTargetStepName( int streamIndex ) {
    StreamInterface stream = getStepIOMeta().getTargetStreams().get( streamIndex );
    return java.util.stream.Stream.of( stream.getStepname(), stream.getSubject() )
      .filter( Objects::nonNull )
      .findFirst().map( Object::toString ).orElse( null );
  }

  public String getAccessTokenField() {
    return accessTokenField;
  }

  public void setAccessTokenField( String accessTokenField ) {
    this.accessTokenField = accessTokenField;
  }

  public String getSourceFilesField() {
    return sourceFilesField;
  }

  public void setSourceFilesField( String sourceFilesField ) {
    this.sourceFilesField = sourceFilesField;
  }

  public String getTargetFilesField() {
    return targetFilesField;
  }

  public void setTargetFilesField( String targetFilesField ) {
    this.targetFilesField = targetFilesField;
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }
  private void readData( Node stepnode ) {
    setSuccessfulStepname( XMLHandler.getTagValue( stepnode, "sendSuccessfulTo" ) );
    setFailedStepname( XMLHandler.getTagValue( stepnode, "sendFailedTo" ) );
    accessTokenField = XMLHandler.getTagValue( stepnode, "accessTokenField" );
    sourceFilesField = XMLHandler.getTagValue( stepnode, "sourceFilesField" );
    targetFilesField = XMLHandler.getTagValue( stepnode, "targetFilesField" );
  }

  public void setDefault() {

  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    try {
      setSuccessfulStepname( rep.getStepAttributeString( id_step, "sendSuccessfulTo" ) );
      setFailedStepname( rep.getStepAttributeString( id_step, "sendFailedTo" ) );
      accessTokenField = rep.getStepAttributeString( id_step, "accessTokenField" );
      sourceFilesField = rep.getStepAttributeString( id_step, "sourceFilesField" );
      targetFilesField = rep.getStepAttributeString( id_step, "targetFilesField" );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "DropboxOutputMeta.Exception.UnexpectedErrorInReadingStepInfoFromRepository" ), e );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "sendSuccessfulTo", getSuccessfulStepname() ) );
    retval.append( "    " + XMLHandler.addTagValue( "sendFailedTo", getFailedStepname() ) );
    retval.append( "    " + XMLHandler.addTagValue( "accessTokenField", accessTokenField ) );
    retval.append( "    " + XMLHandler.addTagValue( "sourceFilesField", sourceFilesField ) );
    retval.append( "    " + XMLHandler.addTagValue( "targetFilesField", targetFilesField ) );
    return retval.toString();
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "sendSuccessfulTo", getSuccessfulStepname() );
      rep.saveStepAttribute( id_transformation, id_step, "sendFailedTo", getFailedStepname() );
      rep.saveStepAttribute( id_transformation, id_step, "accessTokenField", accessTokenField );
      rep.saveStepAttribute( id_transformation, id_step, "sourceFilesField", sourceFilesField );
      rep.saveStepAttribute( id_transformation, id_step, "targetFilesField", targetFilesField );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "DropboxOutputMeta.Exception.UnableToSaveStepInfoToRepository", id_step ), e );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
    StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
    RowMetaInterface info, VariableSpace space, Repository repository, 
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG, "DropboxOutputMeta.CheckResult.NotReceivingFields" ), stepMeta ); 
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DropboxOutputMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );  
      remarks.add( cr );
    }
    
    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "DropboxOutputMeta.CheckResult.StepRecevingData2" ), stepMeta ); 
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG, "DropboxOutputMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ); 
      remarks.add( cr );
    }
  }
  
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
    return new DropboxOutput( stepMeta, stepDataInterface, cnr, tr, trans );
  }
  
  public StepDataInterface getStepData() {
    return new DropboxOutputData();
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamInterface.StreamType.TARGET, null, BaseMessages.getString(
        PKG, "DropboxOutputMeta.InfoStream.Successful.Description" ), StreamIcon.TRUE, null ) );
      ioMeta.addStream( new Stream( StreamInterface.StreamType.TARGET, null, BaseMessages.getString(
        PKG, "DropboxOutputMeta.InfoStream.Failed.Description" ), StreamIcon.FALSE, null ) );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetStepIoMeta() {
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream
   *          The optional stream to handle.
   */
  public void handleStreamSelection( StreamInterface stream ) {
    // This step targets another step.
    // Make sure that we don't specify the same step for true and false...
    // If the user requests false, we blank out true and vice versa
    //
    List<StreamInterface> targets = getStepIOMeta().getTargetStreams();
    int index = targets.indexOf( stream );
    if ( index == 0 ) {
      // True
      //
      StepMeta failedStep = targets.get( 1 ).getStepMeta();
      if ( failedStep != null && failedStep.equals( stream.getStepMeta() ) ) {
        targets.get( 1 ).setStepMeta( null );
      }
    }
    if ( index == 1 ) {
      // False
      //
      StepMeta succesfulStep = targets.get( 0 ).getStepMeta();
      if ( succesfulStep != null && succesfulStep.equals( stream.getStepMeta() ) ) {
        targets.get( 0 ).setStepMeta( null );
      }
    }
  }

  public String getDialogClassName() {
    return "com.leonardo.coelho.dropboxoutput.DropboxOutputDialog";
  }
}

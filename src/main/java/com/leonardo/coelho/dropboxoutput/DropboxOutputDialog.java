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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.Arrays;

public class DropboxOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = DropboxOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int ELEMENT_SPACING = Const.MARGIN;

  private DropboxOutputMeta meta;

  private ScrolledComposite scrolledComposite;
  private Composite contentComposite;

  // Step Name.
  private Label wStepNameLabel;
  private Text wStepNameField;

  // Group Transfer Content.
  private Group transferGroup;

  // OAuth Access Token
  private Label wAccessTokenLabel;
  private CCombo wAccessTokenField;

  // Files to be transferred.
  private Label wSourceFilesLabel;
  private CCombo wSourceFilesComboBox;

  // Target folder.
  private Label wTargetFilesLabel;
  private CCombo wTargetFilesComboBox;

  // Footer Buttons
  private Button wCancel;
  private Button wOK;

  // Listeners
  private ModifyListener lsMod;
  private Listener lsCancel;
  private Listener lsOK;
  private SelectionAdapter lsDef;
  private boolean changed;

  public DropboxOutputDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (DropboxOutputMeta) in;
  }

  public String open() {
    //Set up window
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, meta );
    int middle = props.getMiddlePct();

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        meta.setChanged();
      }
    };
    changed = meta.hasChanged();

    //15 pixel margins
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.Shell.Title" ) );

    //Build a scrolling composite and a composite for holding all content
    scrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL );
    contentComposite = new Composite( scrolledComposite, SWT.NONE );
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginRight = MARGIN_SIZE;
    contentComposite.setLayout( contentLayout );
    FormData compositeLayoutData = new FormDataBuilder().fullSize()
                                                        .result();
    contentComposite.setLayoutData( compositeLayoutData );
    props.setLook( contentComposite );

    //Step name label and text field
    wStepNameLabel = new Label( contentComposite, SWT.RIGHT );
    wStepNameLabel.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.Stepname.Label" ) );
    props.setLook( wStepNameLabel );
    FormData fdStepNameLabel = new FormDataBuilder().left()
      .top()
      .right( middle, -ELEMENT_SPACING )
      .result();
    wStepNameLabel.setLayoutData( fdStepNameLabel );

    wStepNameField = new Text( contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepNameField.setText( stepname );
    props.setLook( wStepNameField );
    wStepNameField.addModifyListener( lsMod );
    FormData fdStepName = new FormDataBuilder().left( middle, 0 )
      .top( )
      .right( 100, 0 )
      .result();
    wStepNameField.setLayoutData( fdStepName );

    // Spacer between entry info and content
    Label topSpacer = new Label( contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormDataBuilder().fullWidth()
                                             .top( wStepNameField, MARGIN_SIZE )
                                             .result();
    topSpacer.setLayoutData( fdSpacer );


    // Group for Transfer Fields.
    transferGroup = new Group( contentComposite, SWT.SHADOW_ETCHED_IN );
    transferGroup.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.Transfer.GroupText" ) );
    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = MARGIN_SIZE;
    groupLayout.marginHeight = MARGIN_SIZE;
    transferGroup.setLayout( groupLayout );
    FormData groupLayoutData = new FormDataBuilder().fullWidth()
      .top( topSpacer, MARGIN_SIZE )
      .result();
    transferGroup.setLayoutData( groupLayoutData );
    props.setLook( transferGroup );

    //Access Token Output label/field
    wAccessTokenLabel = new Label( transferGroup, SWT.RIGHT );
    props.setLook( wAccessTokenLabel );
    wAccessTokenLabel.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.AccessToken.Label" ) );
    FormData fdlTransformation = new FormDataBuilder().left()
      .top()
      .right( middle, -ELEMENT_SPACING )
      .result();
    wAccessTokenLabel.setLayoutData( fdlTransformation );

    wAccessTokenField = new CCombo( transferGroup, SWT.BORDER );
    props.setLook( wAccessTokenField );
    wAccessTokenField.addModifyListener( lsMod );
    FormData fdTransformation = new FormDataBuilder().left( middle, 0 )
      .top()
      .right( 100, 0 )
      .result();
    wAccessTokenField.setLayoutData( fdTransformation );

    // Source Files label/field
    wSourceFilesLabel = new Label( transferGroup, SWT.RIGHT );
    props.setLook( wSourceFilesLabel );
    wSourceFilesLabel.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.SourceFiles.Label" ) );
    FormData fdlTransformation2 = new FormDataBuilder().left()
      .top( wAccessTokenLabel, ELEMENT_SPACING )
      .right( middle, -ELEMENT_SPACING )
      .result();
    wSourceFilesLabel.setLayoutData( fdlTransformation2 );

    wSourceFilesComboBox = new CCombo( transferGroup, SWT.BORDER );
    props.setLook( wSourceFilesComboBox );
    wSourceFilesComboBox.addModifyListener( lsMod );
    FormData fdTransformation2 = new FormDataBuilder().left( middle, 0 )
      .top( wAccessTokenField, ELEMENT_SPACING )
      .right( 100, 0 )
      .result();
    wSourceFilesComboBox.setLayoutData( fdTransformation2 );

    // Target Folder label/field
    wTargetFilesLabel = new Label( transferGroup, SWT.RIGHT );
    props.setLook( wTargetFilesLabel );
    wTargetFilesLabel.setText( BaseMessages.getString( PKG, "DropboxOutputDialog.TargetFolder.Label" ) );
    FormData fdlTransformation3 = new FormDataBuilder().left()
      .top( wSourceFilesLabel, ELEMENT_SPACING )
      .right( middle, -ELEMENT_SPACING )
      .result();
    wTargetFilesLabel.setLayoutData( fdlTransformation3 );

    wTargetFilesComboBox = new CCombo( transferGroup, SWT.BORDER );
    props.setLook( wTargetFilesComboBox );
    wTargetFilesComboBox.addModifyListener( lsMod );
    FormData fdTransformation3 = new FormDataBuilder().left( middle, 0 )
      .top( wSourceFilesComboBox, ELEMENT_SPACING )
      .right( 100, 0 )
      .result();
    wTargetFilesComboBox.setLayoutData( fdTransformation3 );

    //Cancel, action and OK buttons for the bottom of the window.
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormDataBuilder().right( 100, -MARGIN_SIZE )
                                             .bottom()
                                             .result();
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormDataBuilder().right( wCancel, -ELEMENT_SPACING )
                                         .bottom()
                                         .result();
    wOK.setLayoutData( fdOk );

    //Space between bottom buttons and the table, final layout for table
    Label bottomSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormDataBuilder().left()
                                              .right( 100, -MARGIN_SIZE )
                                              .bottom( wCancel, -MARGIN_SIZE )
                                              .result();
    bottomSpacer.setLayoutData( fdhSpacer );

    //Add everything to the scrolling composite
    scrolledComposite.setContent( contentComposite );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setMinSize( contentComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    scrolledComposite.setLayout( new FormLayout() );
    FormData fdScrolledComposite = new FormDataBuilder().fullWidth()
                                                        .top()
                                                        .bottom( bottomSpacer, -MARGIN_SIZE )
                                                        .result();
    scrolledComposite.setLayoutData( fdScrolledComposite );
    props.setLook( scrolledComposite );

    //Listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wStepNameField.addSelectionListener( lsDef );

    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    //Show shell
    setSize();

    // Populate Window.
    getData();
    meta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    // Add previous fields to the combo box options.
    try {
      String[] prevFields = transMeta.getPrevStepFields( stepname ).getFieldNames();
      Arrays.stream( prevFields ).forEach( field -> {
        wAccessTokenField.add( field );
        wSourceFilesComboBox.add( field );
        wTargetFilesComboBox.add( field );
      } );
    } catch ( KettleStepException e ) {
      e.printStackTrace();
    }

    String accessTokenField = meta.getAccessTokenField();
    if ( accessTokenField != null ) {
      wAccessTokenField.setText( meta.getAccessTokenField() );
    }
    String sourceFilesField = meta.getSourceFilesField();
    if ( sourceFilesField != null ) {
      wSourceFilesComboBox.setText( meta.getSourceFilesField() );
    }
    String targetFolderField = meta.getTargetFilesField();
    if ( targetFolderField != null ) {
      wTargetFilesComboBox.setText( meta.getTargetFilesField() );
    }
  }

  /**
   * Save information from dialog fields to the meta-data input.
   */
  private void getMeta( DropboxOutputMeta meta ) {
    meta.setAccessTokenField( wAccessTokenField.getText() );
    meta.setSourceFilesField( wSourceFilesComboBox.getText() );
    meta.setTargetFilesField( wTargetFilesComboBox.getText() );
  }

  private void cancel() {
    meta.setChanged( changed );
    dispose();
  }

  private void ok() {
    getMeta( meta );
    stepname = wStepNameField.getText();
    dispose();
  }
}

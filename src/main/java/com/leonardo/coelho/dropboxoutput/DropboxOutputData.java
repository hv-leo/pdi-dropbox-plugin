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

import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


public class DropboxOutputData extends BaseStepData implements StepDataInterface {
  int accessTokenIdx;
  int sourceFileIdx;
  int targetFilesIdx;

  static final long CHUNKED_UPLOAD_CHUNK_SIZE = 8L << 20; // 8MiB
  static final int CHUNKED_UPLOAD_MAX_ATTEMPTS = 5;

  /**
   * 
   */
  public DropboxOutputData() {
    super();
  }
}

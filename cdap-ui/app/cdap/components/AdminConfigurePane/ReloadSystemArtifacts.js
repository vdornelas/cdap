/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, { Component } from 'react';
import ConfigureButton from 'components/ConfigureButton';
import ConfirmationModal from 'components/ConfirmationModal';
import {MyArtifactApi} from 'api/artifact';
import T from 'i18n-react';

const PREFIX = 'features.Administration.Configure.buttons.ReloadSystemArtifacts';

export default class ReloadSystemArtifacts extends Component {
  constructor(props) {
    super(props);

    this.state = {
      openConfirmation: false,
      loading: false,
      errorMessage: null,
      extendedMessage: null
    };

    this.onClick = this.onClick.bind(this);
    this.reload = this.reload.bind(this);
  }

  onClick() {
    this.setState({
      openConfirmation: !this.state.openConfirmation,
      loading: false,
      errorMessage: null,
      extendedMessage: null
    });
  }

  reload() {
    this.setState({
      loading: true
    });

    MyArtifactApi.reloadSystemArtifacts()
      .subscribe(() => {
        this.onClick();
      }, (err) => {
        this.setState({
          loading: false,
          errorMessage: T.translate(`${PREFIX}.errorMessage`),
          extendedMessage: err
        });
      });
  }

  render() {
    return (
      <span>
        <ConfigureButton
          label={T.translate(`${PREFIX}.label`)}
          onClick={this.onClick}
          iconClass="icon-artifacts"
        />

        <ConfirmationModal
          headerTitle={T.translate(`${PREFIX}.confirmationHeader`)}
          toggleModal={this.onClick}
          confirmationText={T.translate(`${PREFIX}.confirmationText`)}
          confirmButtonText={T.translate(`${PREFIX}.confirmationButton`)}
          confirmFn={this.reload}
          cancelFn={this.onClick}
          isOpen={this.state.openConfirmation}
          isLoading={this.state.loading}
          errorMessage={this.state.errorMessage}
          extendedMessage={this.state.extendedMessage}
        />
      </span>
    );
  }
}

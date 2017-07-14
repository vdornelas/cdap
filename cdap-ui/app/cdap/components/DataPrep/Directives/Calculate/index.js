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

import React, {Component, PropTypes} from 'react';
import classnames from 'classnames';
import T from 'i18n-react';
import {Input} from 'reactstrap';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import ScrollableList from 'components/ScrollableList';
import IconSVG from 'components/IconSVG';
import WarningContainer from 'components/WarningContainer';
import {columnNameAlreadyExists} from 'components/DataPrep/helper';
import {preventPropagation} from 'services/helpers';

require('./Calculate.scss');

const PREFIX = 'features.DataPrep.Directives.Calculate';

export default class Calculate extends Component {
  constructor(props) {
    super(props);

    this.defaultState = {
      operationPopoverOpen: null,
      operationInput : 1,
      createNewColumn: false,
      newColumnInput: this.props.column + T.translate(`${PREFIX}.newColumnInputSuffix`)
    };

    this.state = Object.assign({}, this.defaultState);

    this.handleKeyPress = this.handleKeyPress.bind(this);
    this.setOperationInput = this.setOperationInput.bind(this);
    this.setNewColumnInput = this.setNewColumnInput.bind(this);
    this.setDefaultState = this.setDefaultState.bind(this);
    this.toggleCreateNewColumn = this.toggleCreateNewColumn.bind(this);
    this.getExpressionAndApply = this.getExpressionAndApply.bind(this);

    this.CALCULATE_OPTIONS = [
      {
        name: 'ADD',
        onClick: this.popoverOptionClick.bind(this, 'ADD')
      },
      {
        name: 'SUBTRACT',
        onClick: this.popoverOptionClick.bind(this, 'SUBTRACT')
      },
      {
        name: 'MULTIPLY',
        onClick: this.popoverOptionClick.bind(this, 'MULTIPLY')
      },
      {
        name: 'DIVIDE',
        onClick: this.popoverOptionClick.bind(this, 'DIVIDE')
      },
      {
        name: 'MODULO',
        onClick: this.popoverOptionClick.bind(this, 'MODULO')
      },
      {
        name: 'divider'
      },
      {
        name: 'POWEROF',
        onClick: this.popoverOptionClick.bind(this, 'POWEROF')
      },
      {
        name: 'SQUARE',
        onClick: this.applyDirective.bind(this, `math:pow(${this.props.column}, 2)`)
      },
      {
        name: 'SQUAREROOT',
        onClick: this.applyDirective.bind(this, `math:sqrt(${this.props.column})`)
      },
      {
        name: 'CUBE',
        onClick: this.applyDirective.bind(this, `math:pow(${this.props.column}, 3)`)
      },
      {
        name: 'CUBEROOT',
        onClick: this.applyDirective.bind(this, `math:cbrt(${this.props.column})`)
      },
      {
        name: 'LOG',
        onClick: this.applyDirective.bind(this, `math:log10(${this.props.column})`)
      },
      {
        name: 'NATURALLOG',
        onClick: this.applyDirective.bind(this, `math:log(${this.props.column})`)
      },
      {
        name: 'divider'
      },
      {
        name: 'ABSVALUE',
        onClick: this.applyDirective.bind(this, `math:abs(${this.props.column})`)
      },
      {
        name: 'CEIL',
        onClick: this.applyDirective.bind(this, `math:ceil(${this.props.column})`)
      },
      {
        name: 'FLOOR',
        onClick: this.applyDirective.bind(this, `math:floor(${this.props.column})`)
      },
      {
        name: 'divider'
      },
      {
        name: 'SIN',
        onClick: this.applyDirective.bind(this, `math:sin(${this.props.column})`)
      },
      {
        name: 'COS',
        onClick: this.applyDirective.bind(this, `math:cos(${this.props.column})`)
      },
      {
        name: 'TAN',
        onClick: this.applyDirective.bind(this, `math:tan(${this.props.column})`)
      },
      {
        name: 'ARCCOS',
        onClick: this.applyDirective.bind(this, `math:acos(${this.props.column})`)
      },
      {
        name: 'ARCSIN',
        onClick: this.applyDirective.bind(this, `math:asin(${this.props.column})`)
      },
      {
        name: 'ARCTAN',
        onClick: this.applyDirective.bind(this, `math:atan(${this.props.column})`)
      },
      {
        name: 'divider'
      },
      {
        name: 'ROUND',
        onClick: this.applyDirective.bind(this, `math:round(${this.props.column})`)
      },
      {
        name: 'RANDOM',
        onClick: this.applyDirective.bind(this, `math:random(${this.props.column})`)
      }
    ];

    this.OPTIONS_WITH_POPOVER = ['ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'MODULO', 'POWEROF'];
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('calculate-directive'));
  }

  componentWillReceiveProps(nextProps) {
    if (!nextProps.isOpen) {
      this.setDefaultState();
    }
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode === 13 && (!this.state.createNewColumn || (this.state.createNewColumn && this.state.newColumnInput.length > 0))) {
      this.getExpressionAndApply();
    }
  }

  setDefaultState() {
    this.setState(this.defaultState);
  }

  setOperationInput(e) {
    let value = parseInt(e.target.value, 10);
    if (typeof value !== 'number' || isNaN(value)) {
      return;
    }

    this.setState({
      operationInput: value
    });
  }

  setNewColumnInput(e) {
    this.setState({
      newColumnInput: e.target.value
    });
  }

  toggleCreateNewColumn() {
    this.setState({
      createNewColumn: !this.state.createNewColumn
    });
  }

  popoverOptionClick(option) {
    let operationPopoverOpen = option === this.state.operationPopoverOpen ? null : option;

    let newState = Object.assign({}, this.defaultState, {
      operationPopoverOpen
    });

    this.setState(newState);
  }

  getExpressionAndApply() {
    let expression;

    switch (this.state.operationPopoverOpen) {
      case 'ADD':
        expression = `${this.props.column} + ${this.state.operationInput}`;
        break;
      case 'SUBTRACT':
        expression = `${this.props.column} - ${this.state.operationInput}`;
        break;
      case 'MULTIPLY':
        expression = `${this.props.column} * ${this.state.operationInput}`;
        break;
      case 'DIVIDE':
        expression = `${this.props.column} / ${this.state.operationInput}`;
        break;
      case 'MODULO':
        expression = `${this.props.column} % ${this.state.operationInput}`;
        break;
      case 'POWEROF':
        expression = `math:pow(${this.props.column}, ${this.state.operationInput})`;
        break;
    }

    this.applyDirective(expression);
  }

  applyDirective(expression) {
    let destinationColumn = this.props.column;
    if (this.state.createNewColumn && this.state.newColumnInput.length > 0) {
      destinationColumn = this.state.newColumnInput;
    }
    let directive = `set-column ${destinationColumn} ${expression}`;

    execute([directive])
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
      }, (err) => {
        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      }
    );
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="second-level-popover"
        onClick={preventPropagation}
      >
        <div className="calculate-options">
          <ScrollableList>
            {
              this.CALCULATE_OPTIONS.map((option) => {
                if (option.name === 'divider') {
                  return (
                    <div className="column-action-divider calculate-options-divider">
                      <hr />
                    </div>
                  );
                }
                return (
                  <div
                    key={option.name}
                    className={classnames('option', {
                      'active': this.state.operationPopoverOpen === option.name
                    })}
                    onClick={option.onClick}
                  >
                    <span>
                      {T.translate(`${PREFIX}.OptionsLabels.${option.name}`)}
                    </span>
                    {
                      this.OPTIONS_WITH_POPOVER.indexOf(option.name) !== -1 ?
                        (
                          <span className="float-xs-right">
                            <span className="fa fa-caret-right" />
                          </span>
                        )
                      : null
                    }
                    {
                      this.state.operationPopoverOpen === option.name ?
                        this.renderThirdLevelPopover()
                      : null
                    }
                  </div>
                );
              })
            }
          </ScrollableList>
        </div>
      </div>
    );
  }

  renderNewColumnNameInput() {
    if (!this.state.createNewColumn) { return null; }

    return (
      <div>
        <div>{T.translate(`${PREFIX}.newColumnInputLabel`)}</div>
        <div className="input">
          <Input
            type="text"
            className="form-control"
            value={this.state.newColumnInput}
            onChange={this.setNewColumnInput}
            onKeyPress={this.handleKeyPress}
            placeholder={T.translate(`${PREFIX}.newColumnInputPlaceholder`)}
            autoFocus
          />
        </div>

        {
          columnNameAlreadyExists(this.state.newColumnInput) ? (
            <WarningContainer
              message={T.translate(`${PREFIX}.newColumnInputDuplicate`)}
            />
          ) : null
        }
      </div>
    );
  }

  renderThirdLevelPopover() {
    if (!this.state.operationPopoverOpen) { return null; }

    let operationSign;

    switch (this.state.operationPopoverOpen) {
      case 'ADD':
        operationSign = '+';
        break;
      case 'SUBTRACT':
        operationSign = '-';
        break;
      case 'MULTIPLY':
        operationSign = 'x';
        break;
      case 'DIVIDE':
        operationSign = '/';
        break;
      case 'MODULO':
        operationSign = '%';
        break;
    }

    return (
      <div
        className="third-level-popover"
        onClick={preventPropagation}
      >
        {
          this.state.operationPopoverOpen === 'POWEROF' ?
            (
              <div className="operation-description">
                {T.translate(`${PREFIX}.Options.POWEROF.description`)}
              </div>
            )
          : null
        }
        <div className="operation-sign-input">
          <span className="operation-sign">
            {operationSign}
          </span>
          <Input
            className="operation-input"
            value={this.state.operationInput}
            onChange={this.setOperationInput}
            onKeyPress={this.handleKeyPress}
            type="number"
            autoFocus
          />
        </div>
        <div
          className="create-new-column-line"
          onClick={this.toggleCreateNewColumn}
        >
          <span className="fa fa-fw">
            <IconSVG
              name={this.state.createNewColumn ? 'icon-check-square' : 'icon-square-o'}
            />
          </span>

          <span>
            {T.translate(`${PREFIX}.createNewColumn`)}
          </span>
        </div>

        {this.renderNewColumnNameInput()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            disabled={this.state.createNewColumn && this.state.newColumnInput.length === 0}
            onClick={this.getExpressionAndApply}
          >
            {T.translate('features.DataPrep.Directives.apply')}
          </button>

          <button
            className="btn btn-link float-xs-right"
            onClick={this.setDefaultState}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </div>
      </div>
    );
  }

  render() {
    return (
      <div
        id="calculate-directive"
        className={classnames('clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>
          {T.translate(`${PREFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}
Calculate.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.string,
  close: PropTypes.func,
  onComplete: PropTypes.func
};

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

import React, {PropTypes, Component} from 'react';
import classnames from 'classnames';
import T from 'i18n-react';
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';
import {preventPropagation} from 'services/helpers';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import IconSVG from 'components/IconSVG';

require('./MarkAsError.scss');

const PREFIX = 'features.DataPrep.Directives.MarkAsError';
const conditionsOptions = [
  'EMPTY',
  'TEXTEXACTLY',
  'TEXTCONTAINS',
  'TEXTSTARTSWITH',
  'TEXTENDSWITH',
  'TEXTREGEX',
  'divider',
  'CUSTOMCONDITION'
];
export default class MarkAsError extends Component {
  state = {
    selectedCondition: conditionsOptions[0],
    conditionValue: '',
    customCondition: '',
    ignoreCase: false
  };

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('mark-as-error-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT' && this.state.conditionValue.length === 0 && this.conditionValueRef) {
      this.conditionValueRef.focus();
    } else if (this.state.selectedCondition.substr(0, 6) === 'CUSTOM' && this.state.customCondition.length === 0 && this.customConditionRef) {
      this.customConditionRef.focus();
    }
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }

  applyDirective = () => {
    execute([this.getDirectiveExpression()])
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
      }, (err) => {
        console.log('error', err);

        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      });
  };

  handleConditionValueChange = (e) => {
    this.setState({conditionValue: e.target.value});
  };

  handleCustomFilterChange = (e) => {
    this.setState({customFilter: e.target.value});
  };

  toggleIgnoreCase = () => {
    this.setState({
      ignoreCase: !this.state.ignoreCase
    });
  };

  getDirectiveExpression = () => {
    let directive;
    let condition = 'send-to-error';
    let column = this.props.column;
    let textValue = this.state.conditionValue;

    switch (this.state.selectedCondition) {
      case 'EMPTY':
        directive = `${condition} empty(${column})`;
        break;
      case 'TEXTCONTAINS':
        if (this.state.ignoreCase) {
          textValue = `(?i)${textValue}`;
        }
        directive = `${condition} "${column}" =~ "${textValue}.*"`;
        break;
      case 'TEXTSTARTSWITH':
        if (this.state.ignoreCase) {
          textValue = `(?i)${textValue}`;
        }
        directive = `${condition} "${column}" =^ "${textValue}"`;
        break;
      case 'TEXTENDSWITH':
        if (this.state.ignoreCase) {
          textValue = `(?i)${textValue}`;
        }
        directive = `${condition} "${column}" =$ "${textValue}"`;
        break;
      case 'TEXTEXACTLY':
        if (this.state.ignoreCase) {
          textValue = `(?i)${textValue}`;
        }
        directive = `${condition} "${column}" == "${textValue}$"`;
        break;
      case 'TEXTREGEX':
        directive = `${condition} "${column}" "${textValue}"`;
        break;
      case 'CUSTOMCONDITION':
        directive = `${condition} "${column}" "${this.state.customCondition}"`;
        break;
    }
    return directive;
  }

  renderTextCondition = () => {
    if (this.state.selectedCondition.substr(0, 4) !== 'TEXT') { return null; }

    let ignoreCase;
    if (this.state.selectedCondition !== 'TEXTREGEX') {
      ignoreCase = (
        <div>
          <span
            className="cursor-pointer"
            onClick={this.toggleIgnoreCase}
          >
            <IconSVG
              className="fa"
              name={this.state.ignoreCase ? "icon-check-square-o" : "icon-square-o"}
            />
            <span>
              {T.translate(`${PREFIX}.ignoreCase`)}
            </span>
          </span>
        </div>
      );
    }

    return (
      <div>
        <br />
        <div>
          <input
            type="text"
            className="form-control mousetrap"
            value={this.state.conditionValue}
            onChange={this.handleConditionValueChange}
            placeholder={T.translate(`${PREFIX}.Placeholders.${this.state.selectedCondition}`)}
            ref={ref => this.conditionValueRef = ref}
          />
        </div>
        {ignoreCase}
      </div>
    );
  };

  renderCustomFilter = () => {
    if (this.state.selectedCondition.substr(0, 6) !== 'CUSTOM') { return null; }

    return (
      <div>
        <br />
        <textarea
          className="form-control"
          value={this.state.customCondition}
          onChange={this.handleCustomFilterChange}
          ref={ref => this.customConditionRef = ref}
          placeholder={T.translate(`${PREFIX}.Placeholders.CUSTOMCONDITION`)}
        />
      </div>
    );
  };

  setCondition = (e) => {
    this.setState({
      selectedCondition: e.target.value
    });
  };

  renderCondition = () => {
    let markAsConditions = conditionsOptions.map((id) => {
      return {
        id,
        displayText: T.translate(`${PREFIX}.Conditions.${id}`)
      };
    });
    return (
      <div>
        <div className="mark-as-error-condition">
          <div className="condition-select">
            <strong>{T.translate(`${PREFIX}.if`)}</strong>
            <div>
              <select
                className="form-control mousetrap"
                value={this.state.selectedCondition}
                onChange={this.setCondition}
              >
                {
                  markAsConditions.map(condition => {
                    if (condition.id === 'divider') {
                      return (
                        <option
                          disabled="disabled"
                          role="separator"
                        >
                          &#x2500;&#x2500;&#x2500;&#x2500;
                        </option>
                      );
                    }
                    return (
                      <option
                        value={condition.id}
                        key={condition.id}
                      >
                        {condition.displayText}
                      </option>
                    );
                  })
                }
              </select>
            </div>
          </div>
        </div>
        {this.renderTextCondition()}
        {this.renderCustomFilter()}
      </div>
    );
  };

  isApplyDisabled = () => {
    if (this.state.selectedCondition.substr(0, 4) === 'TEXT') {
      return this.state.conditionValue.length === 0;
    }

    if (this.state.selectedCondition.substr(0, 4) === 'CUSTOM') {
      return this.state.customCondition.length === 0;
    }
  };

  renderDetail = () => {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="filter-detail second-level-popover"
        onClick={preventPropagation}
      >
        {this.renderCondition()}

        <hr />

        <div className="action-buttons">
          <button
            className="btn btn-primary float-xs-left"
            onClick={this.applyDirective}
            disabled={this.isApplyDisabled()}
          >
            {T.translate('features.DataPrep.Directives.apply')}
          </button>

          <button
            className="btn btn-link float-xs-right"
            onClick={this.props.close}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </div>
      </div>
    );
  };

  render() {
    return (
      <div
        id="mark-as-error-directive"
        className={classnames('clearfix action-item', {
          'active': this.state.isOpen
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className="float-xs-right">
          <IconSVG
            name="icon-caret-right"
            className="fa"
          />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

MarkAsError.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};

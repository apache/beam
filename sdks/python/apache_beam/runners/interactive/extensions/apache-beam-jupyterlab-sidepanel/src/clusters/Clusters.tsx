// Licensed under the Apache License, Version 2.0 (the 'License'); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

import * as React from 'react';

import { ISessionContext } from '@jupyterlab/apputils';

import { Button } from '@rmwc/button';
import {
  DataTable,
  DataTableBody,
  DataTableContent,
  DataTableHead,
  DataTableHeadCell,
  DataTableRow
} from '@rmwc/data-table';

import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  DialogButton
} from '@rmwc/dialog';

import { Fab } from '@rmwc/fab';

import { Select } from '@rmwc/select';

import {
  TopAppBar,
  TopAppBarFixedAdjust,
  TopAppBarRow,
  TopAppBarSection,
  TopAppBarTitle
} from '@rmwc/top-app-bar';

import { KernelModel } from '../kernel/KernelModel';

import '@rmwc/button/styles';
import '@rmwc/data-table/styles';
import '@rmwc/dialog/styles';
import '@rmwc/fab/styles';
import '@rmwc/select/styles';
import '@rmwc/top-app-bar/styles';

interface IClustersProps {
  sessionContext: ISessionContext;
}

interface IClustersState {
  kernelDisplayName: string;
  clusters: object;
  defaultClusterId: string;
  selectedId: string;
  selectedName: string;
  showDialog: boolean;
  displayTable: boolean;
}

/**
 * This component is an interactive data-table that inspects Dataproc clusters
 * managed by Apache Beam.
 *
 * The following user functionality is provided in this component:
 * 1. View all Dataproc clusters managed by Interactive Beam
 * 2. Delete a selected cluster. This will "reset" the cluster used by the
 *    corresponding pipelines and the cluster will be recreated when the
 *    pipeline is executed again.
 * 3. Select a default cluster. This is the cluster that will be used by
 *    Interactive Beam when no master_url or Google Cloud project is
 *    specified in the pipeline options.
 */
export class Clusters extends React.Component<IClustersProps, IClustersState> {
  constructor(props: IClustersProps) {
    super(props);
    this._inspectKernelCode = 'ie.current_env().inspector.list_clusters()';
    this._model = new KernelModel(this.props.sessionContext);
    this.state = {
      kernelDisplayName: 'no kernel',
      clusters: this._model.executeResult,
      defaultClusterId: '',
      selectedId: '',
      selectedName: '',
      showDialog: false,
      displayTable: true
    };
  }

  componentDidMount(): void {
    this._queryKernelTimerId = setInterval(
      () => this.queryKernel(this._inspectKernelCode),
      2000
    );
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1000);
    this._updateSessionInfoTimerId = setInterval(
      () => this.updateSessionInfo(),
      2000
    );
  }

  componentWillUnmount(): void {
    clearInterval(this._queryKernelTimerId);
    clearInterval(this._updateRenderTimerId);
    clearInterval(this._updateSessionInfoTimerId);
  }

  queryKernel(code: string): void {
    this._model.execute(code);
  }

  updateRender(): void {
    const clustersToUpdate = this._model.executeResult;
    if (Object.keys(clustersToUpdate).length) {
      this.setState({ displayTable: true });
      if (
        JSON.stringify(this.state.clusters) !== JSON.stringify(clustersToUpdate)
      ) {
        this.setState({ clusters: clustersToUpdate });
      }
    }
  }

  updateSessionInfo(): void {
    if (this.props.sessionContext) {
      const newKernelDisplayName = this.props.sessionContext.kernelDisplayName;
      if (newKernelDisplayName !== this.state.kernelDisplayName) {
        this.setState({
          kernelDisplayName: newKernelDisplayName
        });
      }
    }
  }

  setDefaultCluster(cluster_id: string): void {
    const setDefaultClusterCode =
      'ie.current_env().clusters.set_default_cluster' +
      `(ie.current_env().inspector.get_cluster_master_url('${cluster_id}'))`;
    this.queryKernel(setDefaultClusterCode);
    this.setState({ defaultClusterId: cluster_id });
  }

  displayDialog(open: boolean, key: string, clusterName: string): void {
    this.setState({
      showDialog: open,
      selectedId: key,
      selectedName: clusterName
    });
  }

  deleteCluster(cluster_id: string): void {
    const deleteClusterCode =
      'ie.current_env().clusters.cleanup' +
      `(ie.current_env().inspector.get_cluster_master_url('${cluster_id}'))`;
    this.queryKernel(deleteClusterCode);
    if (this.state.defaultClusterId === cluster_id) {
      const resetDefaultClusterCode =
        'ie.current_env().clusters.default_cluster_metadata=None';
      this.queryKernel(resetDefaultClusterCode);
      this.setState({ defaultClusterId: '' });
    }
    if (Object.keys(this.state.clusters).length === 1) {
      this.setState({ displayTable: false });
    }
  }

  render(): React.ReactNode {
    if (Object.keys(this.state.clusters).length && this.state.displayTable) {
      const clusterNames: any[] = [];
      const clusters = Object.entries(this.state.clusters).map(
        ([key, value]) => {
          const pipelines = value['pipelines'].join(', ');
          clusterNames.push({
            label:
              `Cluster: ${value['cluster_name']}, ` +
              `Project: ${value['project']}, ` +
              `Region: ${value['region']}`,
            value: key
          });
          return (
            <DataTableRow key={key}>
              <DataTableHeadCell>{value['cluster_name']}</DataTableHeadCell>
              <DataTableHeadCell>{value['project']}</DataTableHeadCell>
              <DataTableHeadCell>{value['region']}</DataTableHeadCell>
              <DataTableHeadCell>{value['master_url']}</DataTableHeadCell>
              <DataTableHeadCell>{pipelines}</DataTableHeadCell>
              <DataTableHeadCell>
                <Button
                  onClick={e => {
                    e.preventDefault();
                    window.location.href = value['dashboard'];
                  }}
                >
                  Dashboard
                </Button>
              </DataTableHeadCell>
              <DataTableHeadCell>
                <Fab
                  icon="delete"
                  style={{ backgroundColor: 'var(--mdc-theme-error)' }}
                  theme={['onError']}
                  mini
                  onClick={e => {
                    this.displayDialog(true, key, value['cluster_name']);
                  }}
                />
              </DataTableHeadCell>
            </DataTableRow>
          );
        }
      );
      return (
        <React.Fragment>
          <TopAppBar fixed dense>
            <TopAppBarRow>
              <TopAppBarSection>
                <TopAppBarTitle>
                  Clusters [kernel:{this.state.kernelDisplayName}]
                </TopAppBarTitle>
              </TopAppBarSection>
            </TopAppBarRow>
          </TopAppBar>
          <TopAppBarFixedAdjust />
          <Select
            label="Default cluster"
            enhanced
            options={clusterNames}
            onChange={e => this.setDefaultCluster(e.currentTarget.value)}
            value={this.state.defaultClusterId}
          />
          <Dialog
            open={this.state.showDialog}
            onClose={e => {
              this.displayDialog(false, '', '');
            }}
          >
            <DialogTitle>Confirm Cluster Deletion</DialogTitle>
            <DialogContent>
              Are you sure you want to delete {this.state.selectedName}?
            </DialogContent>
            <DialogActions>
              <DialogButton isDefaultAction action="close">
                Cancel
              </DialogButton>
              <DialogButton
                action="accept"
                onClick={e => {
                  this.deleteCluster(this.state.selectedId);
                }}
              >
                Delete
              </DialogButton>
            </DialogActions>
          </Dialog>
          <div className="Clusters">
            <DataTable>
              <DataTableContent>
                <DataTableHead>
                  <DataTableRow>
                    <DataTableHeadCell>Cluster</DataTableHeadCell>
                    <DataTableHeadCell>Project</DataTableHeadCell>
                    <DataTableHeadCell>Region</DataTableHeadCell>
                    <DataTableHeadCell>Master URL</DataTableHeadCell>
                    <DataTableHeadCell>Pipelines</DataTableHeadCell>
                    <DataTableHeadCell>Dashboard Link</DataTableHeadCell>
                  </DataTableRow>
                </DataTableHead>
                <DataTableBody>{clusters}</DataTableBody>
              </DataTableContent>
            </DataTable>
          </div>
        </React.Fragment>
      );
    }
    return <div>No clusters detected.</div>;
  }

  private _inspectKernelCode: string;
  private _model: KernelModel;
  private _queryKernelTimerId: number;
  private _updateRenderTimerId: number;
  private _updateSessionInfoTimerId: number;
}

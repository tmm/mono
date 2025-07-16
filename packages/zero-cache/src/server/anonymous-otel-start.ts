import {type Meter} from '@opentelemetry/api';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {resourceFromAttributes} from '@opentelemetry/resources';
import type {ObservableResult} from '@opentelemetry/api';
import {platform} from 'os';
import {h64} from '../../../shared/src/hash.js';
import type {LogContext} from '@rocicorp/logger';
import packageJson from '../../../zero/package.json' with {type: 'json'};
import {getZeroConfig, type ZeroConfig} from '../config/zero-config.js';
import {execSync} from 'child_process';
import {randomUUID} from 'crypto';
import {existsSync, readFileSync, writeFileSync, mkdirSync} from 'fs';
import {join, dirname} from 'path';
import {homedir} from 'os';

interface TelemetryMetrics {
  mutations: number;
  rowsSynced: number;
  connectionsSuccess: number;
  connectionsAttempted: number;
  pid: number;
  workerType: string | undefined;
}

interface TelemetryMessage {
  type: 'telemetry-metrics';
  data: TelemetryMetrics;
}

class AnonymousTelemetryManager {
  static #instance: AnonymousTelemetryManager;
  #started = false;
  #meter!: Meter;
  #meterProvider!: MeterProvider;
  #totalMutations = 0;
  #totalRowsSynced = 0;
  #totalConnectionsSuccess = 0;
  #totalConnectionsAttempted = 0;
  #lc: LogContext | undefined;
  #config: ZeroConfig | undefined;
  #cachedAttributes: Record<string, string> | undefined;
  #isMainProcess: boolean;
  #childMetrics: Map<number, TelemetryMetrics> = new Map();
  #sendToParentInterval: ReturnType<typeof setInterval> | null = null;

  static getInstance(): AnonymousTelemetryManager {
    if (!AnonymousTelemetryManager.#instance) {
      AnonymousTelemetryManager.#instance = new AnonymousTelemetryManager();
    }
    return AnonymousTelemetryManager.#instance;
  }

  constructor() {
    // Determine if this is the main process or a child process
    this.#isMainProcess = !process.send;

    if (!this.#isMainProcess && process.send) {
      // Child process: set up periodic sending of metrics to parent
      this.#sendToParentInterval = setInterval(() => {
        this.#sendMetricsToParent();
      }, 10000); // Send every 10 seconds
    }

    if (this.#isMainProcess && process) {
      // Main process: listen for metrics from child processes
      process.on('message', (message: unknown) => {
        if (
          message &&
          typeof message === 'object' &&
          message !== null &&
          'type' in message &&
          message.type === 'telemetry-metrics'
        ) {
          this.#handleChildMetrics(message as TelemetryMessage);
        }
      });
    }
  }

  #sendMetricsToParent() {
    if (process.send && !this.#isMainProcess) {
      const metrics: TelemetryMetrics = {
        mutations: this.#totalMutations,
        rowsSynced: this.#totalRowsSynced,
        connectionsSuccess: this.#totalConnectionsSuccess,
        connectionsAttempted: this.#totalConnectionsAttempted,
        pid: process.pid,
        workerType: undefined, // TODO: Pass worker type via start() method
      };

      const message: TelemetryMessage = {
        type: 'telemetry-metrics',
        data: metrics,
      };

      try {
        process.send(message);
        this.#lc?.debug?.(
          `Sent telemetry metrics to parent: ${JSON.stringify(metrics)}`,
        );
      } catch (error) {
        this.#lc?.debug?.('Failed to send metrics to parent:', error);
      }
    }
  }

  #handleChildMetrics(message: TelemetryMessage) {
    const {pid, ...metrics} = message.data;
    this.#childMetrics.set(pid, message.data);
    this.#lc?.debug?.(
      `Received telemetry metrics from child ${pid}: ${JSON.stringify(metrics)}`,
    );
  }

  #getAggregatedMetrics() {
    // Start with main process metrics
    let totalMutations = this.#totalMutations;
    let totalRowsSynced = this.#totalRowsSynced;
    let totalConnectionsSuccess = this.#totalConnectionsSuccess;
    let totalConnectionsAttempted = this.#totalConnectionsAttempted;

    // Add metrics from all child processes
    for (const childMetrics of this.#childMetrics.values()) {
      totalMutations += childMetrics.mutations;
      totalRowsSynced += childMetrics.rowsSynced;
      totalConnectionsSuccess += childMetrics.connectionsSuccess;
      totalConnectionsAttempted += childMetrics.connectionsAttempted;
    }

    return {
      totalMutations,
      totalRowsSynced,
      totalConnectionsSuccess,
      totalConnectionsAttempted,
    };
  }

  start(lc?: LogContext, config?: ZeroConfig) {
    if (!config) {
      try {
        config = getZeroConfig();
      } catch (e) {
        this.#lc?.debug?.(
          'Anonymous telemetry disabled: unable to parse config',
          e,
        );
        return;
      }
    }

    if (process.env.DO_NOT_TRACK) {
      this.#lc?.debug?.(
        'Anonymous telemetry disabled: DO_NOT_TRACK environment variable is set',
      );
      return;
    }

    if (this.#started || !config.enableUsageAnalytics) {
      return;
    }

    this.#lc = lc;
    this.#config = config;
    this.#cachedAttributes = undefined;

    // Only start the actual telemetry export in the main process
    if (this.#isMainProcess) {
      const resource = resourceFromAttributes(this.#getAttributes());
      const metricReader = new PeriodicExportingMetricReader({
        exportIntervalMillis: 60000,
        exporter: new OTLPMetricExporter({
          url: 'https://metrics.rocicorp.dev',
        }),
      });

      this.#meterProvider = new MeterProvider({
        resource,
        readers: [metricReader],
      });
      this.#meter = this.#meterProvider.getMeter('zero-anonymous-telemetry');

      this.#setupMetrics();
      this.#lc?.info?.(
        'Anonymous telemetry started in main process (exports every 60 seconds)',
      );
    } else {
      this.#lc?.info?.(
        'Anonymous telemetry started in child process (metrics sent to main process)',
      );
    }

    this.#started = true;
  }

  #setupMetrics() {
    // Only set up actual metrics in the main process
    if (!this.#isMainProcess) return;

    // Observable gauges
    const uptimeGauge = this.#meter.createObservableGauge('zero.uptime', {
      description: 'System uptime in seconds',
      unit: 'seconds',
    });

    // Observable counters
    const uptimeCounter = this.#meter.createObservableCounter(
      'zero.uptime_counter',
      {
        description: 'System uptime in seconds',
        unit: 'seconds',
      },
    );
    const mutationsCounter = this.#meter.createObservableCounter(
      'zero.mutations_processed',
      {
        description: 'Total number of mutations processed',
      },
    );
    const rowsSyncedCounter = this.#meter.createObservableCounter(
      'zero.rows_synced',
      {
        description: 'Total number of rows synced',
      },
    );

    // Observable counters for connections
    const connectionsSuccessCounter = this.#meter.createObservableCounter(
      'zero.connections_success',
      {
        description: 'Total number of successful connections',
      },
    );

    const connectionsAttemptedCounter = this.#meter.createObservableCounter(
      'zero.connections_attempted',
      {
        description: 'Total number of attempted connections',
      },
    );

    // Callbacks
    const attrs = this.#getAttributes();
    uptimeGauge.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`Telemetry: uptime=${uptimeSeconds}s`);
    });
    uptimeCounter.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`Telemetry: uptime_counter=${uptimeSeconds}s`);
    });
    mutationsCounter.addCallback((result: ObservableResult) => {
      const aggregated = this.#getAggregatedMetrics();
      result.observe(aggregated.totalMutations, attrs);
      this.#lc?.debug?.(
        `Telemetry: mutations_processed=${aggregated.totalMutations}`,
      );
    });
    rowsSyncedCounter.addCallback((result: ObservableResult) => {
      const aggregated = this.#getAggregatedMetrics();
      result.observe(aggregated.totalRowsSynced, attrs);
      this.#lc?.debug?.(`Telemetry: rows_synced=${aggregated.totalRowsSynced}`);
    });
    connectionsSuccessCounter.addCallback((result: ObservableResult) => {
      const aggregated = this.#getAggregatedMetrics();
      result.observe(aggregated.totalConnectionsSuccess, attrs);
      this.#lc?.debug?.(
        `Telemetry: connections_success=${aggregated.totalConnectionsSuccess}`,
      );
    });
    connectionsAttemptedCounter.addCallback((result: ObservableResult) => {
      const aggregated = this.#getAggregatedMetrics();
      result.observe(aggregated.totalConnectionsAttempted, attrs);
      this.#lc?.debug?.(
        `Telemetry: connections_attempted=${aggregated.totalConnectionsAttempted}`,
      );
    });
  }

  recordMutation(count = 1) {
    this.#totalMutations += count;
  }

  recordRowsSynced(count: number) {
    this.#totalRowsSynced += count;
  }

  recordConnectionSuccess() {
    this.#totalConnectionsSuccess++;
  }

  recordConnectionAttempted() {
    this.#totalConnectionsAttempted++;
  }

  shutdown() {
    if (this.#sendToParentInterval !== null) {
      clearInterval(this.#sendToParentInterval);
      this.#sendToParentInterval = null;
    }

    // Send final metrics before shutdown if this is a child process
    if (!this.#isMainProcess) {
      this.#sendMetricsToParent();
    }

    if (this.#meterProvider) {
      this.#lc?.info?.('Shutting down anonymous telemetry');
      void this.#meterProvider.shutdown();
    }
  }

  #getAttributes() {
    if (!this.#cachedAttributes) {
      this.#cachedAttributes = {
        'zero.app.id': h64(this.#config?.upstream.db || 'unknown').toString(),
        'zero.machine.os': platform(),
        'zero.telemetry.type': 'anonymous',
        'zero.infra.platform': this.#getPlatform(),
        'zero.version': this.#config?.serverVersion ?? packageJson.version,
        'zero.project.id': this.#getGitProjectId(),
        'zero.fs.id': this.#getOrSetFsID(),
      };
      this.#lc?.debug?.(
        `Telemetry: cached attributes=${JSON.stringify(this.#cachedAttributes)}`,
      );
    }
    return this.#cachedAttributes;
  }

  #getPlatform(): string {
    if (process.env.FLY_APP_NAME || process.env.FLY_REGION) return 'fly.io';
    if (
      process.env.ECS_CONTAINER_METADATA_URI_V4 ||
      process.env.ECS_CONTAINER_METADATA_URI ||
      process.env.AWS_REGION ||
      process.env.AWS_EXECUTION_ENV
    )
      return 'aws';
    if (process.env.RAILWAY_ENV || process.env.RAILWAY_STATIC_URL)
      return 'railway';
    if (process.env.RENDER || process.env.RENDER_SERVICE_ID) return 'render';
    if (
      process.env.GCP_PROJECT ||
      process.env.GCLOUD_PROJECT ||
      process.env.GOOGLE_CLOUD_PROJECT
    )
      return 'gcp';
    if (process.env.COOLIFY_URL || process.env.COOLIFY_CONTAINER_NAME)
      return 'coolify';

    return 'unknown';
  }

  #findUp(startDir: string, target: string): string | null {
    let dir = startDir;
    while (dir !== dirname(dir)) {
      if (existsSync(join(dir, target))) return dir;
      dir = dirname(dir);
    }
    return null;
  }

  #getGitProjectId(): string {
    try {
      const cwd = process.cwd();
      const gitRoot = this.#findUp(cwd, '.git');
      if (!gitRoot) {
        return 'unknown';
      }

      const rootCommitHash = execSync('git rev-list --max-parents=0 HEAD -1', {
        cwd: gitRoot,
        encoding: 'utf8',
        timeout: 1000,
        stdio: ['ignore', 'pipe', 'ignore'], // Suppress stderr
      }).trim();

      return rootCommitHash.length === 40 ? rootCommitHash : 'unknown';
    } catch (error) {
      this.#lc?.debug?.('Unable to get Git root commit:', error);
      return 'unknown';
    }
  }

  #getOrSetFsID(): string {
    try {
      if (this.#isInContainer()) {
        return 'container';
      }

      const fsidPath = join(homedir(), '.rocicorp', 'fsid');
      const fsidDir = dirname(fsidPath);

      if (existsSync(fsidPath)) {
        const existingId = readFileSync(fsidPath, 'utf8').trim();
        return existingId;
      }

      if (!existsSync(fsidDir)) {
        mkdirSync(fsidDir, {recursive: true});
      }

      const newId = randomUUID();
      writeFileSync(fsidPath, newId, 'utf8');
      return newId;
    } catch (error) {
      this.#lc?.debug?.('Unable to get or set filesystem ID:', error);
      return 'unknown';
    }
  }

  #isInContainer(): boolean {
    try {
      if (existsSync('/.dockerenv')) {
        return true;
      }
      if (existsSync('/usr/local/bin/docker-entrypoint.sh')) {
        return true;
      }

      if (process.env.KUBERNETES_SERVICE_HOST) {
        return true;
      }

      if (
        process.env.DOCKER_CONTAINER_ID ||
        process.env.HOSTNAME?.match(/^[a-f0-9]{12}$/)
      ) {
        return true;
      }

      if (existsSync('/proc/1/cgroup')) {
        const cgroup = readFileSync('/proc/1/cgroup', 'utf8');
        if (
          cgroup.includes('docker') ||
          cgroup.includes('kubepods') ||
          cgroup.includes('containerd')
        ) {
          return true;
        }
      }

      return false;
    } catch (error) {
      this.#lc?.debug?.('Unable to detect container environment:', error);
      return false;
    }
  }
}

const manager = () => AnonymousTelemetryManager.getInstance();

export const startAnonymousTelemetry = (lc?: LogContext, config?: ZeroConfig) =>
  manager().start(lc, config);
export const recordMutation = (count = 1) => manager().recordMutation(count);
export const recordRowsSynced = (count: number) =>
  manager().recordRowsSynced(count);
export const recordConnectionSuccess = () =>
  manager().recordConnectionSuccess();
export const recordConnectionAttempted = () =>
  manager().recordConnectionAttempted();
export const shutdownAnonymousTelemetry = () => manager().shutdown();

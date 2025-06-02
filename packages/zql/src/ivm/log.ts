export type LogConfig = {
  level: 'debug' | 'info' | 'warn' | 'error';
  format: 'text' | 'json';
  slowRowThreshold: number;
  slowHydrateThreshold: number;
  ivmSampling: number;
};

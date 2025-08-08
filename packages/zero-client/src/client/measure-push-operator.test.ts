import {describe, expect, test, vi} from 'vitest';
import type {Change} from '../../../zql/src/ivm/change.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import type {
  FetchRequest,
  Input,
  Output,
} from '../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../zql/src/ivm/schema.ts';
import type {MetricsDelegate} from '../../../zql/src/query/metrics-delegate.ts';
import {MeasurePushOperator} from './measure-push-operator.ts';

describe('MeasurePushOperator', () => {
  test('should pass through fetch calls', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );
    const req = {} as FetchRequest;

    measurePushOperator.fetch(req);

    expect(mockInput.fetch).toHaveBeenCalledWith(req);
  });

  test('should pass through cleanup calls', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );
    const req = {} as FetchRequest;

    measurePushOperator.cleanup(req);

    expect(mockInput.cleanup).toHaveBeenCalledWith(req);
  });

  test('should pass through getSchema calls', () => {
    const schema = {} as SourceSchema;
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => schema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );

    const result = measurePushOperator.getSchema();

    expect(result).toBe(schema);
    expect(mockInput.getSchema).toHaveBeenCalled();
  });

  test('should pass through destroy calls', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );

    measurePushOperator.destroy();

    expect(mockInput.destroy).toHaveBeenCalled();
  });

  test('should measure push timing and record metric', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );
    measurePushOperator.setOutput(mockOutput);

    const change: Change = {
      type: 'add',
      node: {} as Node,
    };

    measurePushOperator.push(change);

    expect(mockOutput.push).toHaveBeenCalledWith(change);
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-client',
      expect.any(Number),
      'test-query-id',
    );
  });

  test('should not record metric when output.push throws', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      cleanup: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(() => {
        throw new Error('Test error');
      }),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
    );
    measurePushOperator.setOutput(mockOutput);

    const change: Change = {
      type: 'add',
      node: {} as Node,
    };

    expect(() => measurePushOperator.push(change)).toThrow('Test error');
    expect(mockMetricsDelegate.addMetric).not.toHaveBeenCalled();
  });
});

import type {LogContext} from '@rocicorp/logger';
import type {ZeroConfig} from '../config/zero-config.ts';
import type {FastifyReply, FastifyRequest} from 'fastify';

export async function handleRunQueryRequest(
  lc: LogContext,
  config: ZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  // 1. run `npx analyze-query` with the ast sent to use
  const {ast} = req.body;
  const result = await runAnalyzeQuery(ast);
  res.send(result);
}

#!/usr/bin/env tsx
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {colorConsole} from '../../../shared/src/logging.ts';
import {pgClient} from '../types/pg.ts';
import {readFileSync} from 'fs';
import {resolve} from 'path';
import * as v from '../../../shared/src/valita.ts';
import {indicesConfigSchema} from '../indices/indices-config.ts';

async function main() {
  const args = process.argv.slice(2);

  // Parse command line arguments
  let configPath: string | undefined;
  let appID = 'zero';
  let databaseUrl: string | undefined;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--config' || arg === '-c') {
      configPath = args[++i];
    } else if (arg === '--app-id') {
      appID = args[++i];
    } else if (arg === '--database-url') {
      databaseUrl = args[++i];
    } else if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    }
  }

  if (!configPath) {
    colorConsole.error('Error: --config parameter is required');
    printHelp();
    process.exit(1);
  }

  const dbUrl = databaseUrl || process.env.ZERO_UPSTREAM_DB;
  if (!dbUrl) {
    colorConsole.error(
      'Error: Database URL must be provided via --database-url or ZERO_UPSTREAM_DB environment variable',
    );
    process.exit(1);
  }

  // Read and validate the indices configuration
  const configFile = resolve(configPath);
  let configContent: string;
  try {
    configContent = readFileSync(configFile, 'utf-8');
  } catch (e) {
    colorConsole.error(`Error reading config file ${configFile}: ${e}`);
    process.exit(1);
  }

  let config;
  try {
    const parsed = JSON.parse(configContent);
    config = v.parse(parsed, indicesConfigSchema);
  } catch (e) {
    colorConsole.error(`Invalid indices configuration: ${e}`);
    process.exit(1);
  }

  // Connect to the database
  const lc = createSilentLogContext();
  const sql = pgClient(lc, dbUrl);

  try {
    // Check if the app schema exists
    const schemaExists = await sql`
      SELECT schema_name 
      FROM information_schema.schemata 
      WHERE schema_name = ${appID}
    `;

    if (schemaExists.length === 0) {
      colorConsole.error(
        `Schema "${appID}" does not exist. ` +
          `Please ensure zero-cache has initialized the upstream database first.`,
      );
      process.exit(1);
    }

    // Check if the indices table exists
    const tableExists = await sql`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = ${appID} AND table_name = 'indices'
    `;

    if (tableExists.length === 0) {
      colorConsole.error(
        `Table "${appID}.indices" does not exist. ` +
          `Please ensure zero-cache has initialized the upstream database first.`,
      );
      process.exit(1);
    }

    // Validate that referenced tables exist
    if (config.tables) {
      const tables = await sql`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
      `;

      const availableTables = new Set(tables.map(t => t.table_name));

      for (const tableName of Object.keys(config.tables)) {
        if (!availableTables.has(tableName)) {
          colorConsole.warn(
            `Warning: Table "${tableName}" referenced in indices config does not exist in database`,
          );
        }
      }
    }

    // Deploy the indices configuration
    await sql`
      UPDATE ${sql(appID)}.indices 
      SET indices = ${config}
      WHERE lock = true
    `;

    // Get the computed hash
    const result = await sql`
      SELECT hash FROM ${sql(appID)}.indices
    `;

    const hash = result[0]?.hash;

    colorConsole.info(
      `✅ Successfully deployed indices configuration for ${appID}`,
    );
    colorConsole.info(`   Hash: ${hash}`);

    if (config.tables) {
      const tableCount = Object.keys(config.tables).length;
      const ftsCount = Object.values(config.tables).filter(
        t => t.fulltext && t.fulltext.length > 0,
      ).length;

      colorConsole.info(`   Tables configured: ${tableCount}`);
      colorConsole.info(`   Tables with fulltext indices: ${ftsCount}`);
    }
  } catch (e) {
    colorConsole.error(`Error deploying indices: ${e}`);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

function printHelp() {
  colorConsole.info(`
Usage: deploy-indices --config <path> [options]

Deploy fulltext index configuration to the upstream PostgreSQL database.

Options:
  --config, -c <path>     Path to the indices configuration JSON file (required)
  --app-id <id>           Application ID (default: "zero")
  --database-url <url>    PostgreSQL connection URL (can also use ZERO_UPSTREAM_DB env var)
  --help, -h              Show this help message

Example:
  deploy-indices --config ./indices.json --app-id myapp

Example indices.json:
  {
    "tables": {
      "comments": {
        "fulltext": [
          {
            "columns": ["body", "title"],
            "tokenizer": "unicode61"
          }
        ]
      }
    }
  }
`);
}

main().catch(e => {
  colorConsole.error(`Unexpected error: ${e}`);
  process.exit(1);
});

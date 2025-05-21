import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {test} from 'vitest';
import '../helpers/comparePg.ts';
import {defaultFormat} from '../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {StaticQuery} from '../../../zql/src/query/static-query.ts';
import {staticToRunnable} from '../helpers/static.ts';

const QUERY_STRING = `playlist
  .where(({exists, not}) =>
    not(
      exists('tracks', q =>
        q
          .whereExists('tracks', q =>
            q
              .whereExists('mediaType', q =>
                q
                  .where('id', '>', 740678861)
                  .where('name', 'LIKE', '00E6tYb0O5')
                  .where('name', 'LIKE', 'XBbSnhBZfI')
                  .where('name', 'IS', '2Yaq3pWdPX')
                  .orderBy('id', 'asc')
                  .limit(12),
              )
              .whereExists('playlists', q =>
                q
                  .whereExists('playlists', q =>
                    q
                      .whereExists('tracks', q =>
                        q
                          .whereExists('tracks', q =>
                            q
                              .where('genreId', '<=', 329624507)
                              .orderBy('bytes', 'asc')
                              .orderBy('name', 'desc')
                              .orderBy('milliseconds', 'desc')
                              .orderBy('id', 'asc')
                              .limit(83),
                          )
                          .orderBy('playlistId', 'asc')
                          .orderBy('trackId', 'asc'),
                      )
                      .where('id', 'IS NOT', 1776037770)
                      .where('id', '<', -1336529662)
                      .where('id', '>=', -1913704239)
                      .where('id', 'IS', 1249694438)
                      .orderBy('id', 'asc')
                      .limit(80),
                  )
                  .orderBy('playlistId', 'asc')
                  .orderBy('trackId', 'asc'),
              )
              .where(({exists, not}) =>
                not(
                  exists('album', q =>
                    q
                      .whereExists('tracks', q =>
                        q
                          .where(({exists, not}) =>
                            not(
                              exists('album', q =>
                                q.orderBy('id', 'asc').limit(62),
                              ),
                            ),
                          )
                          .where('name', 'IS', '7vT932F31t')
                          .where('albumId', 'IS NOT', null)
                          .where('mediaTypeId', '!=', 97401641)
                          .orderBy('id', 'asc'),
                      )
                      .where('artistId', '>=', -1971611424)
                      .where('artistId', '>=', 1918035680)
                      .orderBy('id', 'asc')
                      .limit(106),
                  ),
                ),
              )
              .where('mediaTypeId', '<=', 1910037151)
              .orderBy('mediaTypeId', 'desc')
              .orderBy('bytes', 'asc')
              .orderBy('id', 'asc')
              .limit(45),
          )
          .orderBy('playlistId', 'asc')
          .orderBy('trackId', 'asc'),
      ),
    ),
  )
  .where('id', '!=', -327916445)
  .related('tracks', q =>
    q
      .where(({exists, not}) =>
        not(
          exists('playlists', q =>
            q
              .whereExists('playlists', q =>
                q
                  .where('id', '!=', 1679429948)
                  .where('id', 'IS', 1964174724)
                  .where('id', 'IS', 1873847761)
                  .where('id', '>', -2011830521)
                  .orderBy('id', 'asc')
                  .limit(165),
              )
              .orderBy('playlistId', 'asc')
              .orderBy('trackId', 'asc'),
          ),
        ),
      )
      .whereExists('mediaType', q => q.orderBy('id', 'asc'))
      .where('bytes', 'IS NOT', -354095702)
      .where('genreId', '!=', 1005532917)
      .related('genre', q => q.orderBy('id', 'asc').limit(21))
      .related('playlists', q =>
        q
          .where('id', 1402319013)
          .where('name', 'LIKE', 'x0kkVGxOoG')
          .related('tracks', q =>
            q
              .where('milliseconds', '<=', -860483379)
              .where('composer', 'IS NOT', 'EeSAI97Lgi')
              .related('invoiceLines', q =>
                q
                  .where(({exists, not}) =>
                    not(
                      exists('track', q =>
                        q
                          .where('milliseconds', '<', -1008927408)
                          .where('name', 'u7M5k6gOFR')
                          .where('bytes', '>=', -953425706)
                          .where('mediaTypeId', '<', -807751898)
                          .orderBy('name', 'asc')
                          .orderBy('bytes', 'asc')
                          .orderBy('mediaTypeId', 'desc')
                          .orderBy('unitPrice', 'asc')
                          .orderBy('id', 'asc')
                          .limit(168),
                      ),
                    ),
                  )
                  .where(({exists, not}) =>
                    not(
                      exists('invoice', q =>
                        q
                          .whereExists('lines', q =>
                            q
                              .where(({exists, not}) =>
                                not(
                                  exists('track', q =>
                                    q
                                      .orderBy('unitPrice', 'desc')
                                      .orderBy('bytes', 'asc')
                                      .orderBy('albumId', 'desc')
                                      .orderBy('id', 'asc'),
                                  ),
                                ),
                              )
                              .where('quantity', -1597821246)
                              .orderBy('trackId', 'desc')
                              .orderBy('id', 'asc')
                              .limit(166),
                          )
                          .where(({exists, not}) =>
                            not(
                              exists('customer', q =>
                                q
                                  .where('postalCode', 'IS', 'hLlHKE9NEU')
                                  .where('country', 'ILIKE', 'sYyrbAJ4Ql')
                                  .orderBy('postalCode', 'asc')
                                  .orderBy('lastName', 'asc')
                                  .orderBy('id', 'asc'),
                              ),
                            ),
                          )
                          .where('id', '!=', 904824390)
                          .orderBy('invoiceDate', 'asc')
                          .orderBy('id', 'asc')
                          .limit(197),
                      ),
                    ),
                  )
                  .whereExists('customer', q =>
                    q
                      .whereExists('customer', q =>
                        q
                          .where('city', 'IS', 'YckNrEhEAP')
                          .where('postalCode', 'LIKE', 'hEBqMAGrWx')
                          .where('email', 'ZfdUV6rUwW')
                          .where('state', null)
                          .orderBy('address', 'asc')
                          .orderBy('id', 'asc')
                          .limit(10),
                      )
                      .orderBy('id', 'asc'),
                  )
                  .where('quantity', '<=', -1686815402)
                  .where('unitPrice', '<=', 8.4688204824268e307)
                  .where('quantity', '!=', -1592477102)
                  .related('track', q =>
                    q
                      .where('bytes', 'IS', -1966938315)
                      .related('genre', q =>
                        q
                          .where('id', '<', 54635338)
                          .where('name', '40JaOUCzdg')
                          .orderBy('id', 'asc')
                          .limit(83),
                      )
                      .related('invoiceLines', q =>
                        q
                          .where('unitPrice', '<=', 5.526905129674712e307)
                          .where('invoiceId', 'IS NOT', 1104352733)
                          .orderBy('unitPrice', 'asc')
                          .orderBy('id', 'asc')
                          .limit(183),
                      )
                      .orderBy('milliseconds', 'asc')
                      .orderBy('id', 'desc')
                      .orderBy('albumId', 'asc')
                      .orderBy('bytes', 'desc')
                      .limit(126),
                  )
                  .orderBy('unitPrice', 'desc')
                  .orderBy('id', 'asc')
                  .limit(136),
              )
              .orderBy('id', 'asc')
              .limit(27),
          )
          .orderBy('id', 'asc')
          .limit(189),
      )
      .related('album', q =>
        q
          .whereExists('tracks', q =>
            q
              .whereExists('mediaType', q =>
                q
                  .where('name', 'IS', 'qOVaZibWax')
                  .where('name', 'ILIKE', 'E816w4P2HM')
                  .where('name', 'IS', 'wPqSe6TYkk')
                  .orderBy('id', 'asc')
                  .limit(79),
              )
              .where(({exists, not}) =>
                not(
                  exists('playlists', q =>
                    q
                      .whereExists('playlists', q =>
                        q
                          .where('id', -239171408)
                          .where('name', 'IS', 'F6MhpaE0Sa')
                          .orderBy('id', 'asc')
                          .limit(61),
                      )
                      .orderBy('playlistId', 'asc')
                      .orderBy('trackId', 'asc'),
                  ),
                ),
              )
              .where('albumId', '!=', null)
              .where('milliseconds', '<=', 457949290)
              .where('id', -1298069950)
              .orderBy('name', 'desc')
              .orderBy('composer', 'desc')
              .orderBy('id', 'asc')
              .limit(9),
          )
          .where(({exists, not}) =>
            not(
              exists('artist', q =>
                q.where('name', 'ILIKE', 'wPS0Kgp5hP').orderBy('id', 'asc'),
              ),
            ),
          )
          .where('artistId', 'IS', -452813523)
          .where('title', '!=', 'JLvUtsvYHh')
          .where('id', '!=', 710782152)
          .related('tracks', q =>
            q
              .where('mediaTypeId', 'IS', -1738954257)
              .related('invoiceLines', q =>
                q
                  .whereExists('invoice', q =>
                    q
                      .where('billingPostalCode', 'LIKE', 'i04qxZQb74')
                      .where('billingCity', null)
                      .where('billingState', 'vdML2IWqxk')
                      .where('invoiceDate', 1770042512796)
                      .orderBy('billingCity', 'asc')
                      .orderBy('id', 'asc')
                      .limit(196),
                  )
                  .whereExists('track', q =>
                    q
                      .where(({exists, not}) =>
                        not(
                          exists('genre', q =>
                            q
                              .where('name', 'IS', 'WIDPPwQ2ig')
                              .orderBy('id', 'asc')
                              .limit(122),
                          ),
                        ),
                      )
                      .where('genreId', '!=', 952201425)
                      .where('milliseconds', 'IS', 510572856)
                      .where('albumId', 187967627)
                      .orderBy('mediaTypeId', 'asc')
                      .orderBy('composer', 'asc')
                      .orderBy('id', 'asc'),
                  )
                  .whereExists('customer', q =>
                    q
                      .whereExists('customer', q =>
                        q
                          .where('company', 'IS NOT', 'lvPAqOitnH')
                          .orderBy('address', 'desc')
                          .orderBy('city', 'desc')
                          .orderBy('company', 'desc')
                          .orderBy('firstName', 'desc')
                          .orderBy('email', 'asc')
                          .orderBy('id', 'asc')
                          .limit(25),
                      )
                      .orderBy('id', 'asc'),
                  )
                  .where('trackId', '<=', 1327972676)
                  .where('quantity', '>=', -1394175075)
                  .where('trackId', -967311339)
                  .where('trackId', '!=', 1723849088)
                  .related('customer', q =>
                    q
                      .whereExists('supportRep', q =>
                        q
                          .where(({exists, not}) =>
                            not(
                              exists('reportsTo', q =>
                                q
                                  .where(({exists, not}) =>
                                    not(
                                      exists('reportsTo', q =>
                                        q
                                          .where(
                                            'reportsTo',
                                            'IS NOT',
                                            -737563421,
                                          )
                                          .where('title', 'AVvUUjFkXr')
                                          .where('reportsTo', '!=', null)
                                          .orderBy('state', 'desc')
                                          .orderBy('title', 'desc')
                                          .orderBy('reportsTo', 'asc')
                                          .orderBy('birthDate', 'asc')
                                          .orderBy('id', 'desc')
                                          .orderBy('postalCode', 'asc')
                                          .limit(70),
                                      ),
                                    ),
                                  )
                                  .where('country', '!=', 'DjwMnDZecP')
                                  .orderBy('country', 'asc')
                                  .orderBy('fax', 'desc')
                                  .orderBy('title', 'desc')
                                  .orderBy('id', 'asc')
                                  .orderBy('lastName', 'desc')
                                  .orderBy('address', 'asc')
                                  .limit(180),
                              ),
                            ),
                          )
                          .where('city', 'LIKE', null)
                          .where('lastName', 'LIKE', 'iWdgaG1nMF')
                          .where('postalCode', 'IS', 'txFV9Kr4rZ')
                          .orderBy('phone', 'desc')
                          .orderBy('lastName', 'desc')
                          .orderBy('reportsTo', 'desc')
                          .orderBy('firstName', 'desc')
                          .orderBy('id', 'asc')
                          .orderBy('title', 'desc')
                          .orderBy('postalCode', 'asc')
                          .limit(129),
                      )
                      .where('country', '!=', 'eGeK8NdGiB')
                      .related('supportRep', q =>
                        q
                          .where('title', 'LIKE', 'Kr97SJor0K')
                          .where('state', 'IS NOT', null)
                          .where('city', 'IS NOT', 'BUsviMUVUx')
                          .orderBy('id', 'asc')
                          .orderBy('lastName', 'desc')
                          .orderBy('postalCode', 'desc')
                          .orderBy('fax', 'desc')
                          .limit(128),
                      )
                      .orderBy('postalCode', 'asc')
                      .orderBy('id', 'asc')
                      .limit(7),
                  )
                  .related('track', q =>
                    q
                      .where(({exists, not}) =>
                        not(
                          exists('invoiceLines', q =>
                            q
                              .where('invoiceId', 1067394949)
                              .where('invoiceId', '<', 1367052664)
                              .orderBy('id', 'asc')
                              .limit(85),
                          ),
                        ),
                      )
                      .where('genreId', '>=', -1974460796)
                      .where('milliseconds', -903510619)
                      .orderBy('composer', 'desc')
                      .orderBy('id', 'asc')
                      .limit(1),
                  )
                  .orderBy('id', 'asc')
                  .limit(111),
              )
              .orderBy('unitPrice', 'desc')
              .orderBy('mediaTypeId', 'desc')
              .orderBy('milliseconds', 'asc')
              .orderBy('id', 'asc')
              .limit(117),
          )
          .related('artist', q =>
            q
              .where(({exists, not}) =>
                not(
                  exists('albums', q =>
                    q
                      .where('id', '<=', -1680663950)
                      .where('id', '!=', 1847734959)
                      .where('id', 'IS NOT', -2012822361)
                      .where('artistId', '>=', -1626337774)
                      .orderBy('id', 'asc')
                      .limit(169),
                  ),
                ),
              )
              .where('name', 'DKEnA13iFt')
              .where('name', 'ILIKE', '0WtEyszk3y')
              .related('albums', q =>
                q
                  .whereExists('tracks', q =>
                    q
                      .where('unitPrice', '>=', 1.8220959701627214e307)
                      .where('milliseconds', '<=', -1390585083)
                      .orderBy('milliseconds', 'desc')
                      .orderBy('unitPrice', 'desc')
                      .orderBy('mediaTypeId', 'asc')
                      .orderBy('composer', 'desc')
                      .orderBy('id', 'asc')
                      .limit(184),
                  )
                  .whereExists('artist', q =>
                    q
                      .where(({exists, not}) =>
                        not(
                          exists('albums', q =>
                            q
                              .whereExists('artist', q =>
                                q
                                  .where('id', 'IS NOT', -1207465671)
                                  .where('name', 'ILIKE', 'He1eFIokaz')
                                  .orderBy('id', 'asc')
                                  .limit(117),
                              )
                              .where(({exists, not}) =>
                                not(
                                  exists('tracks', q =>
                                    q
                                      .where(({exists, not}) =>
                                        not(
                                          exists('genre', q =>
                                            q
                                              .where(
                                                'name',
                                                'LIKE',
                                                '8nsX0Y2nL1',
                                              )
                                              .where('name', 'P6b5teAmoe')
                                              .orderBy('id', 'asc')
                                              .limit(20),
                                          ),
                                        ),
                                      )
                                      .where('albumId', '<=', 160941015)
                                      .where('genreId', -1729982218)
                                      .where('bytes', 'IS', null)
                                      .where('composer', '!=', 'Qwb57HM0nu')
                                      .orderBy('unitPrice', 'desc')
                                      .orderBy('id', 'asc')
                                      .orderBy('milliseconds', 'desc')
                                      .orderBy('albumId', 'asc')
                                      .limit(125),
                                  ),
                                ),
                              )
                              .where('artistId', '>=', -37965367)
                              .where('title', 'IS', 'lrSGkCd6BZ')
                              .where('id', 129783903)
                              .orderBy('id', 'asc')
                              .limit(138),
                          ),
                        ),
                      )
                      .where('name', 'LIKE', 'iB4PQHT0GI')
                      .where('id', 'IS', -1764256257)
                      .orderBy('id', 'asc')
                      .limit(193),
                  )
                  .where('title', '!=', 'OxZPOq9gYU')
                  .orderBy('id', 'asc')
                  .limit(179),
              )
              .orderBy('id', 'asc')
              .limit(144),
          )
          .orderBy('id', 'asc')
          .limit(8),
      )
      .orderBy('composer', 'asc')
      .orderBy('albumId', 'desc')
      .orderBy('bytes', 'desc')
      .orderBy('mediaTypeId', 'desc')
      .orderBy('id', 'asc')
      .limit(108),
  )`;

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'frontend_analysis',
  zqlSchema: schema,
  pgContent,
});

const z = {
  query: Object.fromEntries(
    Object.entries(schema.tables).map(([name]) => [
      name,
      new StaticQuery(
        schema,
        name as keyof typeof schema.tables,
        {table: name},
        defaultFormat,
      ),
    ]),
  ),
};

const f = new Function('z', `return z.query.${QUERY_STRING};`);
const query: AnyQuery = f(z);

test('manual zql string', async () => {
  await runAndCompare(
    schema,
    staticToRunnable({
      query,
      schema,
      harness,
    }),
    undefined,
  );
});

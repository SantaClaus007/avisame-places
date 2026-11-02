// Loads places for the squares people stopped in, from Overture Maps (20261101000208; owner,
// 2026-11-01: "find something that's scalable... copy Life360", then "yes to both"). Run every ten
// minutes by .github/workflows/places.yml; it can also be run by hand:
//
//   FUNCTIONS_URL=https://<ref>.supabase.co/functions/v1 CRON_SECRET=... node load.mjs
//
// THIS REPOSITORY IS PUBLIC (unlimited free GitHub minutes). Its logs are public too, so they say
// totals only: never which squares, never anything about anybody.
//
// For each square it is handed (functions/load-places, action 'claim'), it reads Overture's public files
// on S3 with DuckDB, only the parts inside that square's box, and hands back (action 'store'):
//  * places: named, still open, 60% sure or more, not something nobody goes to (a parking lot, an ATM,
//    a charger), one per name within 60 m (Overture can list an airport twice), each with how far its
//    building reaches (the building its point is in: half that building's diagonal, at most 200 m);
//  * big places: the outlines of airports, stadiums, campuses, hospitals, malls, parks and the like
//    that reach into the square, simplified to about 5 m, so a stop in an airport garage is the airport.
// Nothing about any person ever reaches this script: only squares.
import { DuckDBInstance } from '@duckdb/node-api';

const FN = (process.env.FUNCTIONS_URL ?? '').replace(/\/$/, '');
const SECRET = process.env.CRON_SECRET ?? '';
const PER_RUN = Number(process.env.PLACES_PER_RUN ?? 6);
const BUCKET = 'https://overturemaps-us-west-2.s3.amazonaws.com';

/** Overture categories nobody "went to". */
const NOT_A_DESTINATION = /(^|_)(parking|atm|ev_charging|charging_station|vending|bus_stop|toilet|public_toilet|mailbox|post_box|bench|drinking_water|taxi_stand|bike_rental_station)($|_)/;
/** Big places a stop inside is named after: Overture base land_use classes, and airports. */
const VENUE_CLASSES = ['stadium', 'theme_park', 'university', 'college', 'school', 'hospital', 'zoo', 'park',
  'golf_course', 'fairground', 'retail', 'convention_center', 'water_park', 'aquarium', 'racetrack'];
const AIRPORT_CLASSES = ['international_airport', 'regional_airport', 'airport', 'military_airport', 'airfield'];

async function call(action, extra = {}) {
  const res = await fetch(`${FN}/load-places`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-cron-secret': SECRET },
    body: JSON.stringify({ action, ...extra }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${action}: ${res.status} ${JSON.stringify(body).slice(0, 200)}`);
  return body;
}

/** The newest Overture release, from the bucket's own listing. */
async function latestRelease() {
  const xml = await (await fetch(`${BUCKET}/?list-type=2&prefix=release/&delimiter=/`)).text();
  const all = [...xml.matchAll(/<Prefix>release\/([0-9.-]+)\/<\/Prefix>/g)].map((m) => m[1]).sort();
  if (!all.length) throw new Error('no Overture release listed');
  return all[all.length - 1];
}

const RAD = Math.PI / 180;
const metres = (a, b) => {
  const dLat = (b[1] - a[1]) * RAD, dLng = (b[0] - a[0]) * RAD;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a[1] * RAD) * Math.cos(b[1] * RAD) * Math.sin(dLng / 2) ** 2;
  return 2 * 6_371_008.8 * Math.asin(Math.min(1, Math.sqrt(h)));
};
/** Area of a ring of [lng, lat], m², on a local flat projection: plenty for "which big place is smaller". */
function ringArea(ring) {
  const lat0 = ring.reduce((s, p) => s + p[1], 0) / ring.length;
  const k = 111_320 * Math.cos(lat0 * RAD);
  let a = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) a += (ring[j][0] * k) * (ring[i][1] * 111_320) - (ring[i][0] * k) * (ring[j][1] * 111_320);
  return Math.abs(a / 2);
}

/** PLACES_DRY="40.70,-84.15;38.15,-85.75": load these squares and only print what would be stored. */
const DRY = (process.env.PLACES_DRY ?? '').split(';').filter(Boolean).map((cell) => {
  const [s, w] = cell.split(',').map(Number);
  return { cell, south: s, west: w, north: Math.round((s + 0.05) * 100) / 100, east: Math.round((w + 0.05) * 100) / 100 };
});

async function main() {
  if (!DRY.length && (!FN || !SECRET)) throw new Error('FUNCTIONS_URL and CRON_SECRET are required');
  const { cells } = DRY.length ? { cells: DRY } : await call('claim', { limit: PER_RUN });
  if (!cells?.length) { console.log('no squares waiting'); return; }
  const release = await latestRelease();
  const R = `s3://overturemaps-us-west-2/release/${release}`;
  const db = await DuckDBInstance.create(':memory:');
  const c = await db.connect();
  const rows = async (sql) => {
    const r = await c.runAndReadAll(sql);
    const cols = r.columnNames();
    return r.getRows().map((row) => Object.fromEntries(row.map((v, i) => [cols[i], typeof v === 'bigint' ? Number(v) : v])));
  };
  // Overture's footers (where each file's row groups sit) are read once and kept for every square.
  await rows("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial; SET s3_region='us-west-2'; SET enable_http_metadata_cache=true; SET enable_object_cache=true;");
  console.log(`Overture ${release}: ${cells.length} squares`);
  let totalPlaces = 0, totalBig = 0, failed = 0;

  for (const cell of cells) {
    const { south: s, west: w, north: n, east: e } = cell;
    const t0 = Date.now();
    try {
      // Places whose point is in the square, with the building each point stands in.
      const places = await rows(`
        WITH p AS (
          SELECT id, names."primary" AS name, basic_category AS cat, confidence, geometry
          FROM read_parquet('${R}/theme=places/type=place/*', hive_partitioning=1)
          WHERE bbox.xmin >= ${w} AND bbox.xmin < ${e} AND bbox.ymin >= ${s} AND bbox.ymin < ${n}
            AND confidence >= 0.6 AND names."primary" IS NOT NULL
            AND coalesce(operating_status, 'open') = 'open'
        ), b AS (
          SELECT id AS bid, geometry, bbox FROM read_parquet('${R}/theme=buildings/type=building/*', hive_partitioning=1)
          WHERE bbox.xmax > ${w} AND bbox.xmin < ${e} AND bbox.ymax > ${s} AND bbox.ymin < ${n}
        )
        SELECT p.id, p.name, p.cat, p.confidence, ST_X(p.geometry) AS lng, ST_Y(p.geometry) AS lat,
               max(b.bbox.xmax - b.bbox.xmin) AS bw, max(b.bbox.ymax - b.bbox.ymin) AS bh, max(b.bid) AS bid
        FROM p LEFT JOIN b ON ST_Contains(b.geometry, p.geometry)
        GROUP BY ALL`);
      // Who a shared building belongs to. A store with its own services inside ("Tool & Truck Rental at
      // The Home Depot", "Kroger Money", "WALMART #006931") is the ANCHOR: it keeps the building's size
      // and the look-alikes are left out as parts of it. A building unrelated places share (an airport
      // terminal, a mall) is nobody's own: none takes its size, or a stop in the airport garage would
      // read "Alamo Rent A Car"; the big place's outline names the stop instead.
      const norm = (x) => String(x).toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^the/, '');
      const byBuilding = new Map();
      for (const p of places) if (p.bid) byBuilding.set(p.bid, [...(byBuilding.get(p.bid) ?? []), p]);
      const anchorOf = new Map();
      for (const [bid, list] of byBuilding) {
        if (list.length < 2) continue;
        const named = list.filter((a) => norm(a.name).length >= 4 && list.some((b) => b !== a && norm(b.name).includes(norm(a.name))));
        named.sort((a, b) => norm(a.name).length - norm(b.name).length || b.confidence - a.confidence);
        anchorOf.set(bid, named[0] ?? null);
      }
      const kept = [];
      for (const p of places.sort((a, b) => b.confidence - a.confidence)) {
        const name = String(p.name).trim();
        if (!name || name.length > 160 || /^[0-9\s#-]+$/.test(name)) continue;
        if (p.cat && NOT_A_DESTINATION.test(String(p.cat))) continue;
        const key = name.toLowerCase().replace(/[^a-z0-9]/g, '');
        if (kept.some((k) => k.key === key && metres([k.lng, k.lat], [p.lng, p.lat]) < 60)) continue;
        const list = p.bid ? byBuilding.get(p.bid) ?? [] : [];
        const anchor = p.bid ? anchorOf.get(p.bid) : undefined;
        // A part of the anchor store: not a place of its own.
        if (anchor && anchor !== p && norm(p.name).includes(norm(anchor.name))) continue;
        const own = list.length <= 1 || anchor === p || (anchor === null && list.length === 2);
        const reach = p.bw == null || !own ? 0 : Math.min(200, Math.round(Math.hypot(p.bw * 111_320 * Math.cos(p.lat * RAD), p.bh * 111_320) / 2));
        kept.push({ key, id: String(p.id), name, category: p.cat ?? null, lat: p.lat, lng: p.lng, reach });
      }
      // Big places that reach into the square.
      const venues = await rows(`
        SELECT id, name, class, xmin, xmax, ymin, ymax, ST_AsGeoJSON(ST_SimplifyPreserveTopology(geometry, 0.00005)) AS gj FROM (
          SELECT id, names."primary" AS name, class, bbox.xmin AS xmin, bbox.xmax AS xmax, bbox.ymin AS ymin, bbox.ymax AS ymax, geometry
          FROM read_parquet('${R}/theme=base/type=land_use/*', hive_partitioning=1)
          WHERE bbox.xmax > ${w} AND bbox.xmin < ${e} AND bbox.ymax > ${s} AND bbox.ymin < ${n}
            AND names."primary" IS NOT NULL AND class IN (${VENUE_CLASSES.map((x) => `'${x}'`).join(',')})
          UNION ALL
          SELECT id, names."primary", class, bbox.xmin, bbox.xmax, bbox.ymin, bbox.ymax, geometry
          FROM read_parquet('${R}/theme=base/type=infrastructure/*', hive_partitioning=1)
          WHERE bbox.xmax > ${w} AND bbox.xmin < ${e} AND bbox.ymax > ${s} AND bbox.ymin < ${n}
            AND subtype = 'airport' AND names."primary" IS NOT NULL AND class IN (${AIRPORT_CLASSES.map((x) => `'${x}'`).join(',')})
        ) WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON')`);
      const outlines = [];
      for (const v of venues) {
        const g = JSON.parse(v.gj);
        const polys = g.type === 'Polygon' ? [g.coordinates] : g.type === 'MultiPolygon' ? g.coordinates : [];
        const rings = polys.map((poly) => poly[0]).filter((r) => r && r.length >= 4 && r.length <= 2000)
          .map((r) => r.map(([x, y]) => [Math.round(x * 1e6) / 1e6, Math.round(y * 1e6) / 1e6]));
        if (!rings.length) continue;
        outlines.push({ id: String(v.id), name: String(v.name).slice(0, 160), class: v.class,
          minLat: v.ymin, maxLat: v.ymax, minLng: v.xmin, maxLng: v.xmax,
          area: Math.round(rings.reduce((a, r) => a + ringArea(r), 0)), rings });
      }
      if (DRY.length) {
        console.log(cell.cell, kept.length, 'places,', kept.filter((p) => p.reach > 0).length, 'with a building;', outlines.length, 'big places:',
          outlines.map((o) => `${o.name} (${o.class}, ${Math.round(o.area / 1e4)} ha)`).slice(0, 12).join('; '));
        console.log('  sample:', kept.slice(0, 12).map((p) => `${p.name} [${p.category}] ${p.reach}m`).join('; '));
        continue;
      }
      const { stored } = await call('store', { cell: cell.cell, places: kept.map(({ key, ...p }) => p), venues: outlines });
      totalPlaces += Number(stored) || 0; totalBig += outlines.length;
      void t0;
    } catch (err) {
      failed += 1;
      // The error's kind only: a message could carry a square.
      console.error(`a square failed: ${err instanceof Error ? err.name : 'unknown'}`);
      await call('fail', { cell: cell.cell }).catch(() => null);
    }
  }
  console.log(`${cells.length} squares: ${totalPlaces} places, ${totalBig} big places, ${failed} failed`);
}

main().catch((e) => { console.error(e instanceof Error ? e.message : e); process.exit(1); });

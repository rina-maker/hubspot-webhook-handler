export const runtime = "nodejs";

/**
 * Env helpers
 */
function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function isoUtcHoursAgo(hours) {
  const d = new Date(Date.now() - hours * 60 * 60 * 1000);
  return d.toISOString().replace(/\.\d{3}Z$/, "Z"); // trim millis
}

function safeJsonParse(text) {
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    return { _raw: text };
  }
}

/**
 * Cin7
 */
async function cin7FetchJson(url) {
  const username = requireEnv("CIN7_USERNAME");
  const key = requireEnv("CIN7_KEY");

  const auth = Buffer.from(`${username}:${key}`).toString("base64");

  const resp = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Basic ${auth}`,
      Accept: "application/json",
    },
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Cin7 error ${resp.status}: ${text}`);
  }

  return safeJsonParse(text);
}

/**
 * HubSpot
 */
async function hubspotFetchJson(path, { method = "GET", body } = {}) {
  const token = requireEnv("HUBSPOT_PRIVATE_APP_TOKEN");

  const resp = await fetch(`https://api.hubapi.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`HubSpot error ${resp.status}: ${text}`);
  }

  return safeJsonParse(text);
}

async function getHubSpotOrderPropertyNames() {
  const props = await hubspotFetchJson("/crm/v3/properties/orders");
  return new Set((props?.results || []).map((p) => p.name));
}

function pickExistingProps(allNamesSet, candidateProps) {
  const out = {};
  for (const [k, v] of Object.entries(candidateProps)) {
    if (v === undefined || v === null || v === "") continue;
    if (allNamesSet.has(k)) out[k] = v;
  }
  return out;
}

/**
 * True idempotency:
 * Batch upsert Orders by a unique property (cin7_order_id).
 * If an order with that cin7_order_id exists -> update.
 * If not -> create.
 */
async function batchUpsertOrders(inputs, idProperty = "cin7_order_id") {
  return hubspotFetchJson("/crm/v3/objects/orders/batch/upsert", {
    method: "POST",
    body: { idProperty, inputs },
  });
}

export async function GET() {
  const startedAt = new Date().toISOString();

  try {
    // ---- Config ----
    const baseUrl = requireEnv("CIN7_BASE_URL").replace(/\/$/, "");
    const path = requireEnv("CIN7_SALES_ORDERS_PATH"); // e.g. "/SalesOrders"

    const lookbackHours = Number(process.env.CIN7_LOOKBACK_HOURS || "48");
    const forcedSince = process.env.CIN7_FORCE_SINCE;

    const since =
      forcedSince && forcedSince.trim()
        ? forcedSince.trim()
        : isoUtcHoursAgo(lookbackHours);

    // Pagination controls
    const rows = Number(process.env.CIN7_ROWS || "250"); // try 250 (safe-ish). If Cin7 rejects, set to 50.
    const maxPages = Number(process.env.CIN7_MAX_PAGES || "50");

    // ---- HubSpot props ----
    const hsOrderProps = await getHubSpotOrderPropertyNames();
    const UNIQUE_PROP = "cin7_order_id";

    if (!hsOrderProps.has(UNIQUE_PROP)) {
      throw new Error(
        `Missing HubSpot Order property "${UNIQUE_PROP}". Create it in HubSpot (Settings → Properties → Orders) as a single-line text field (unique if possible), then redeploy.`
      );
    }

    // ---- Build Cin7 filter (matches what worked in Postman) ----
    const where = `invoiceDate >= '${since}' AND invoiceDate IS NOT NULL`;

    console.log("[cin7-sync] started", {
      startedAt,
      since,
      rows,
      maxPages,
      forcedSince: Boolean(forcedSince && forcedSince.trim()),
    });

    // ---- Fetch Cin7 pages ----
    let page = 1;
    const allOrders = [];

    while (true) {
      const cin7Url =
        `${baseUrl}${path}` +
        `?where=${encodeURIComponent(where)}` +
        `&fields=id,reference,invoiceDate,stage,deliveryFirstName,deliveryLastName,deliveryCompany,deliveryAddress1,deliveryAddress2,deliveryCity,deliveryState,deliveryPostalCode,deliveryCountry,freightTotal,productTotal,total` +
        `&order=invoiceDate` +
        `&rows=${rows}` +
        `&page=${page}`;

      console.log("[cin7-sync] fetching page", { page });

      const cin7Data = await cin7FetchJson(cin7Url);
      const orders = Array.isArray(cin7Data)
        ? cin7Data
        : (cin7Data?.items || cin7Data?.results || []);

      if (!orders.length) break;

      allOrders.push(...orders);
      page += 1;

      if (page > maxPages) {
        console.log("[cin7-sync] reached maxPages safety valve", { maxPages });
        break;
      }
    }

    console.log("[cin7-sync] fetched from cin7", { count: allOrders.length });

    // ---- Map to HubSpot upsert inputs ----
    const nowIso = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");

    const inputs = [];
    let skipped = 0;

    for (const o of allOrders) {
      const cin7OrderId = String(o?.id || o?.Id || "").trim();
      if (!cin7OrderId) {
        skipped++;
        continue;
      }

      // Totals — your Postman fields include productTotal / total / freightTotal
      const orderTotal =
        o?.total ?? o?.Total ?? o?.productTotal ?? o?.ProductTotal ?? null;

      const candidateProps = {
        [UNIQUE_PROP]: cin7OrderId,

        // Helpful auditing fields (create these in HubSpot if you want to filter by them)
        cin7_reference: o?.reference ?? null,
        cin7_invoice_date: o?.invoiceDate ?? null,
        cin7_last_synced_at: nowIso,

        // Common Order fields (only written if they exist in your portal)
        hs_order_number: o?.reference ?? cin7OrderId,
        hs_total_price: orderTotal,
        hs_currency_code: "USD",

        // Shipping info (matches your Cin7 Postman fields)
        ship_to_first_name: o?.deliveryFirstName ?? null,
        ship_to_last_name: o?.deliveryLastName ?? null,
        ship_to_company: o?.deliveryCompany ?? null,
        ship_to_address: o?.deliveryAddress1 ?? null,
        ship_to_address2: o?.deliveryAddress2 ?? null,
        ship_to_city: o?.deliveryCity ?? null,
        ship_to_state: o?.deliveryState ?? null,
        ship_to_postal_code: o?.deliveryPostalCode ?? null,
        ship_to_country: o?.deliveryCountry ?? null,
      };

      const properties = pickExistingProps(hsOrderProps, candidateProps);

      inputs.push({
        id: cin7OrderId, // <-- upsert key value
        properties,
      });
    }

    console.log("[cin7-sync] prepared upsert inputs", {
      prepared: inputs.length,
      skipped,
    });

    // ---- Batch upsert to HubSpot ----
    const chunkSize = 100;
    let upserted = 0;
    const errors = [];

    for (let i = 0; i < inputs.length; i += chunkSize) {
      const chunk = inputs.slice(i, i + chunkSize);

      try {
        const res = await batchUpsertOrders(chunk, UNIQUE_PROP);
        upserted += res?.results?.length || 0;
      } catch (e) {
        errors.push(String(e?.message || e));
      }
    }

    const finishedAt = new Date().toISOString();

    console.log("[cin7-sync] complete", {
      startedAt,
      finishedAt,
      since,
      cin7Count: allOrders.length,
      upsertPrepared: inputs.length,
      upserted,
      skipped,
      errorsCount: errors.length,
      sampleErrors: errors.slice(0, 3),
    });

    return Response.json({
      ok: true,
      startedAt,
      finishedAt,
      since,
      cin7Fetched: allOrders.length,
      upsertPrepared: inputs.length,
      upserted,
      skipped,
      errorsCount: errors.length,
      sampleErrors: errors.slice(0, 3),
    });
  } catch (err) {
    console.error("[cin7-sync] failed", err);
    return Response.json(
      { ok: false, error: String(err?.message || err) },
      { status: 500 }
    );
  }
}
